//! Pools of temporary machines that cloudlease creates and deletes
//!
//! A pool is a CI class whose machines exist only while its jobs need them.
//! The daemon that creates a pool (its coordinator) asks cloudlease for
//! machines when ready jobs of the class find no free slot, and each machine
//! installs Homebased at boot, joins the coordinator, offers the class, and
//! renews its own lease only while it is useful. cloudlease deletes a machine
//! whose lease ends; the coordinator drops it from the Fleet once cloudlease
//! confirms it is gone. See `docs/temporary-machines.md`

pub mod boot;
pub mod cloudlease;
pub mod error;
pub mod renewal;

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::AppError;
use crate::fleet::address::MachineAddress;
use crate::machine::{MachineId, MachineName};
use crate::queue::ci::{JobClass, SlotCapacity};
pub use error::{PoolCode, PoolError};

/// Most machines one pool may have at once
pub const MAX_MACHINES: u32 = 64;

/// Longest a pool may live, counted from its creation
pub const MAX_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

/// Idle time before a machine with no work releases itself, by default
pub const DEFAULT_IDLE_LIMIT: Duration = Duration::from_secs(10 * 60);

/// Time without output after which running work no longer keeps a machine, by default
pub const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(30 * 60);

/// Time from create to join after which a machine is released, by default
pub const DEFAULT_BOOT_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// A pool's name, which is also the CI class its machines offer
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PoolName(JobClass);

impl PoolName {
    /// The CI class the pool's machines offer and its jobs name
    #[must_use]
    pub fn class(&self) -> &JobClass {
        &self.0
    }

    /// The pool named by a CI class
    #[must_use]
    pub fn from_class(class: JobClass) -> Self {
        Self(class)
    }

    /// Name of the machine filling `slot`: the pool name and the slot's last
    /// eight hex digits, which stays one DNS label
    #[must_use]
    pub fn machine_name(&self, slot: SlotId) -> MachineName {
        let simple = slot.0.simple().to_string();
        let tail = &simple[simple.len() - 8..];
        let raw = format!("{}-{tail}", self.0);
        // a class is at most 31 lowercase letters, digits, and hyphens, so
        // the name always parses; the fallback only guards that invariant
        MachineName::from_str(&raw).unwrap_or_else(|_| MachineName::fallback())
    }
}

impl fmt::Display for PoolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for PoolName {
    type Err = AppError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Ok(Self(JobClass::parse(raw)?))
    }
}

/// A cloudlease machine class, such as `do-c16`: size, region, image, and
/// volumes are cloudlease configuration, never Homebased's
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MachineClass(String);

impl MachineClass {
    /// Validate a class name: 1 to 63 lowercase letters, digits, and hyphens
    pub fn parse(raw: &str) -> Result<Self, AppError> {
        let valid = !raw.is_empty()
            && raw.len() <= 63
            && raw
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        if !valid {
            return Err(AppError::Usage {
                message: format!(
                    "machine class {raw:?} must be 1 to 63 lowercase letters, digits, or hyphens"
                ),
            });
        }
        Ok(Self(raw.to_owned()))
    }

    /// The class name
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for MachineClass {
    type Err = AppError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl TryFrom<String> for MachineClass {
    type Error = AppError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<MachineClass> for String {
    fn from(class: MachineClass) -> Self {
        class.0
    }
}

impl fmt::Display for MachineClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! opaque_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wrap an identifier cloudlease issued
            #[must_use]
            pub fn new(raw: impl Into<String>) -> Self {
                Self(raw.into())
            }

            /// The identifier
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

opaque_id!(
    /// cloudlease's identifier of a pool's run
    RunId
);
opaque_id!(
    /// cloudlease's identifier of one machine's lease
    LeaseId
);

/// One machine of a pool, chosen by the coordinator before it asks
/// cloudlease; it is also the create call's `request_id`, so a retry after a
/// lost answer names the same machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SlotId(Uuid);

impl SlotId {
    /// Allocate a new v7 id
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// The UUID
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for SlotId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SlotId {
    type Err = AppError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(raw).map(Self).map_err(|_| AppError::Usage {
            message: format!("{raw:?} is not a machine slot UUID"),
        })
    }
}

/// Single-use secret a machine presents when it joins its coordinator
///
/// It travels in the machine's boot data, which anything on the machine can
/// read back, so it is worth nothing once the machine has joined
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JoinToken(String);

impl JoinToken {
    /// A new token of 256 random bits from the OS
    pub fn generate() -> Result<Self, AppError> {
        use std::io::Read;
        let mut bytes = [0_u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        Ok(Self(
            bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
        ))
    }

    /// Wrap a token read from config
    #[must_use]
    pub fn new(raw: String) -> Self {
        Self(raw)
    }

    /// The token text
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// What the coordinator stores and compares
    #[must_use]
    pub fn hash(&self) -> JoinTokenHash {
        let digest = Sha256::digest(self.0.as_bytes());
        JoinTokenHash(digest.iter().map(|byte| format!("{byte:02x}")).collect())
    }
}

impl fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JoinToken(..)")
    }
}

/// SHA-256 of a join token, hex
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JoinTokenHash(String);

/// Limits and timings a pool's coordinator and machines follow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolSettings {
    /// cloudlease class every machine of the pool has
    pub machine_class: MachineClass,
    /// Most machines alive at once, `releasing` ones included
    pub max_machines: u32,
    /// No machine of the pool lives past this; cloudlease enforces it
    pub deadline_at: DateTime<Utc>,
    /// CI slots of the pool's class on each machine
    pub slots: SlotCapacity,
    /// A machine with no work for this long releases itself
    #[serde(with = "humantime_serde")]
    pub idle_limit: Duration,
    /// Running work with no output for this long no longer renews the lease
    #[serde(with = "humantime_serde")]
    pub stall_after: Duration,
    /// A machine that has not joined this long after its create is released
    #[serde(with = "humantime_serde")]
    pub boot_timeout: Duration,
    /// File on the coordinator of `NAME=value` lines each machine's runs
    /// get, read again at each join; never stored or sent to cloudlease
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<PathBuf>,
}

/// Why a pool stopped taking machines
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum PoolEnd {
    /// Someone ran `pool release`
    Requested,
    /// Its deadline passed
    Deadline,
    /// cloudlease refused the pool's run or a machine for good
    Refused {
        /// cloudlease's error code
        code: String,
        /// cloudlease's message
        message: String,
    },
}

impl PoolEnd {
    /// Error code that ends the pool's waiting jobs
    #[must_use]
    pub fn job_code(&self) -> &str {
        match self {
            Self::Requested => "pool_released",
            Self::Deadline => "pool_deadline",
            Self::Refused { code, .. } => code,
        }
    }

    /// Message that ends the pool's waiting jobs
    #[must_use]
    pub fn job_message(&self, pool: &PoolName) -> String {
        match self {
            Self::Requested => format!("pool {pool} was released before the job got a machine"),
            Self::Deadline => {
                format!("pool {pool} passed its deadline before the job got a machine")
            }
            Self::Refused { code, message } => {
                format!("cloudlease refused pool {pool} ({code}): {message}")
            }
        }
    }
}

/// Where a pool is in its life
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PoolState {
    /// It asks for machines as its jobs need them
    Open,
    /// It asks for no more machines and releases the ones it has
    Releasing {
        /// Why
        #[serde(flatten)]
        end: PoolEnd,
    },
    /// Every machine it had is confirmed gone
    Released {
        /// Why
        #[serde(flatten)]
        end: PoolEnd,
        /// When the last machine was confirmed gone
        at: DateTime<Utc>,
    },
}

impl PoolState {
    /// Storage tag
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Releasing { .. } => "releasing",
            Self::Released { .. } => "released",
        }
    }
}

/// A pool as its coordinator stores it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pool {
    /// Its name and CI class
    pub name: PoolName,
    /// Limits and timings
    pub settings: PoolSettings,
    /// `request_id` of the cloudlease run create, fixed at pool create
    pub run_request: Uuid,
    /// cloudlease's run, once created
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<RunId>,
    /// Where it is in its life
    #[serde(flatten)]
    pub state: PoolState,
    /// When it was created
    pub created_at: DateTime<Utc>,
    /// Last cloudlease error the coordinator met, for `pool show`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Why a machine was or is being deleted
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseReason {
    /// The coordinator asked, for example because its pool was released
    Requested,
    /// The machine had no work for its idle limit
    Idle,
    /// The machine said its work is done
    Done,
    /// The machine stopped renewing, because its work stalled or it died
    HeartbeatLapsed,
    /// It reached its hard deadline; unsaved output on it was lost
    HardDeadline,
    /// Its run was released
    RunReleased,
    /// A spend cap was reached
    SpendCap,
    /// The provider could not create it
    CreateFailed,
    /// It did not join within the pool's boot timeout
    BootTimeout,
    /// cloudlease refused to create it
    Refused,
    /// A reason this release does not know, as cloudlease sent it
    #[serde(untagged)]
    Other(String),
}

impl ReleaseReason {
    /// Parse cloudlease's reason text
    #[must_use]
    pub fn from_wire(raw: &str) -> Self {
        serde_json::from_value(serde_json::Value::String(raw.to_owned()))
            .unwrap_or_else(|_| Self::Other(raw.to_owned()))
    }

    /// Whether the machine may have been deleted under running work
    #[must_use]
    pub fn may_lose_output(&self) -> bool {
        !matches!(self, Self::Idle | Self::Done | Self::Refused)
    }
}

impl fmt::Display for ReleaseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(text)) => f.write_str(&text),
            _ => f.write_str("unknown"),
        }
    }
}

/// The lease a machine runs under, once cloudlease accepted its create
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineLease {
    /// cloudlease's lease
    pub lease: LeaseId,
    /// cloudlease deletes the machine at this time whatever else happens
    pub hard_deadline_at: DateTime<Utc>,
    /// When cloudlease created the lease
    pub created_at: DateTime<Utc>,
}

/// A machine as the Fleet knows it once it joined
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinedMachine {
    /// The machine's Homebased installation
    pub machine: MachineId,
    /// Its iroh endpoint id, bound to its lease by the join; the coordinator
    /// serves no other id for this machine
    pub endpoint: iroh::EndpointId,
    /// Loopback address on the coordinator where its Fleet reaches the
    /// machine through the link; a new one after each coordinator restart
    pub address: MachineAddress,
    /// When it joined
    pub joined_at: DateTime<Utc>,
}

/// Where one machine of a pool is in its life, as its coordinator sees it
///
/// Busy and idle are not states here: they are how many of the machine's
/// CI slots hold runs, which its offer reports
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MachineState {
    /// Saved before the create call; the call may or may not have reached cloudlease
    Requested,
    /// cloudlease holds a lease; the machine has not joined yet
    Booting {
        /// Its lease
        #[serde(flatten)]
        lease: MachineLease,
    },
    /// It joined the Fleet and offers the pool's class
    Joined {
        /// Its lease
        #[serde(flatten)]
        lease: MachineLease,
        /// Its Fleet membership
        joined: JoinedMachine,
    },
    /// Its lease is ending; cloudlease has not confirmed it gone
    Releasing {
        /// Its lease
        #[serde(flatten)]
        lease: MachineLease,
        /// Its Fleet membership, if it joined
        #[serde(default, skip_serializing_if = "Option::is_none")]
        joined: Option<JoinedMachine>,
        /// Why
        reason: ReleaseReason,
    },
    /// cloudlease confirmed every resource of it gone, or never created it
    Released {
        /// Its lease, if cloudlease ever accepted it
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease: Option<MachineLease>,
        /// Its Fleet membership, if it joined
        #[serde(default, skip_serializing_if = "Option::is_none")]
        joined: Option<JoinedMachine>,
        /// Why
        reason: ReleaseReason,
        /// When cloudlease confirmed it gone
        gone_at: DateTime<Utc>,
    },
}

impl MachineState {
    /// Storage tag
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Booting { .. } => "booting",
            Self::Joined { .. } => "joined",
            Self::Releasing { .. } => "releasing",
            Self::Released { .. } => "released",
        }
    }

    /// Its lease, while cloudlease holds one
    #[must_use]
    pub fn lease(&self) -> Option<&MachineLease> {
        match self {
            Self::Requested => None,
            Self::Booting { lease }
            | Self::Joined { lease, .. }
            | Self::Releasing { lease, .. } => Some(lease),
            Self::Released { lease, .. } => lease.as_ref(),
        }
    }

    /// Its Fleet membership, if it joined
    #[must_use]
    pub fn joined(&self) -> Option<&JoinedMachine> {
        match self {
            Self::Joined { joined, .. } => Some(joined),
            Self::Releasing { joined, .. } | Self::Released { joined, .. } => joined.as_ref(),
            Self::Requested | Self::Booting { .. } => None,
        }
    }

    /// Whether it still counts against the pool's machine limit
    #[must_use]
    pub fn is_live(&self) -> bool {
        !matches!(self, Self::Released { .. })
    }
}

/// One machine of a pool
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolMachine {
    /// Its slot, and the create's `request_id`
    pub slot: SlotId,
    /// Its pool
    pub pool: PoolName,
    /// Its Fleet name and hostname
    pub name: MachineName,
    /// Hash of the join token in its boot data
    pub join_token: JoinTokenHash,
    /// When the coordinator decided to ask for it
    pub requested_at: DateTime<Utc>,
    /// Where it is in its life
    #[serde(flatten)]
    pub state: MachineState,
}

/// Why a job ended with its machine: carried by the `JOB_FAILED` its origin
/// writes once cloudlease confirmed the job's temporary machine gone
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineReleased {
    /// The pool
    pub pool: PoolName,
    /// The machine
    pub machine: MachineName,
    /// Why cloudlease deleted it
    pub reason: ReleaseReason,
    /// The machine's hard deadline
    pub hard_deadline_at: DateTime<Utc>,
    /// When cloudlease confirmed it gone
    pub gone_at: DateTime<Utc>,
    /// Anything the job wrote on the machine and had not uploaded is gone
    pub output_lost: bool,
}

/// What `pool show` and `pool list` print
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolView {
    /// The pool
    #[serde(flatten)]
    pub pool: Pool,
    /// Its machines, newest first
    pub machines: Vec<PoolMachine>,
}

/// Body of `POST /v1/cluster/pool/join`, sent by a temporary machine's daemon
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRequest {
    /// Always 1
    pub api_version: u32,
    /// Pool the machine was created for
    pub pool: PoolName,
    /// Its slot
    pub slot: SlotId,
    /// Its Homebased installation
    pub machine: MachineId,
    /// The lease from its `lease.json`
    pub lease: LeaseId,
    /// The single-use token from its boot data
    pub join_token: JoinToken,
}

/// Answer to a join: everything the machine needs that boot data must not carry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinAnswer {
    /// Always 1
    pub api_version: u32,
    /// Pool and CI class it serves
    pub pool: PoolName,
    /// CI slots it offers
    pub slots: SlotCapacity,
    /// Idle time after which it releases itself
    #[serde(with = "humantime_serde")]
    pub idle_limit: Duration,
    /// Time without output after which running work stops renewing
    #[serde(with = "humantime_serde")]
    pub stall_after: Duration,
    /// Variables every run on the machine gets, such as tokens for uploads
    #[serde(default)]
    pub env: Vec<(String, String)>,
}

/// Read `NAME=value` lines: blank lines and `#` comments are skipped, and a
/// name must be a shell variable name
pub fn parse_env_file(text: &str) -> Result<Vec<(String, String)>, String> {
    let mut vars = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            return Err(format!("line {}: expected NAME=value", index + 1));
        };
        let valid = name.starts_with(|first: char| first.is_ascii_alphabetic() || first == '_')
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid || name.starts_with("HOMEBASED_") {
            return Err(format!(
                "line {}: {name:?} is not a usable variable name",
                index + 1
            ));
        }
        vars.push((name.to_owned(), value.to_owned()));
    }
    Ok(vars)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_names_are_one_dns_label_from_pool_and_slot() {
        let pool: PoolName = "afv".parse().unwrap();
        let slot: SlotId = "01a20ead-998b-754a-bcc3-f7a34fa9bd9a".parse().unwrap();
        assert_eq!(pool.machine_name(slot).to_string(), "afv-4fa9bd9a");
        let long: PoolName = "a".repeat(31).parse().unwrap();
        assert_eq!(long.machine_name(slot).to_string().len(), 40);
    }

    #[test]
    fn release_reasons_keep_unknown_wire_text() {
        assert_eq!(
            ReleaseReason::from_wire("hard_deadline"),
            ReleaseReason::HardDeadline
        );
        assert_eq!(
            ReleaseReason::from_wire("provider_gone"),
            ReleaseReason::Other("provider_gone".into())
        );
        assert_eq!(ReleaseReason::Other("x".into()).to_string(), "x");
        assert!(ReleaseReason::HardDeadline.may_lose_output());
        assert!(!ReleaseReason::Idle.may_lose_output());
    }

    #[test]
    fn env_files_take_names_and_refuse_homebased_variables() {
        let vars = parse_env_file("# token\nHF_TOKEN=abc=def\n\nUPLOAD_DIR=/data\n").unwrap();
        assert_eq!(
            vars,
            vec![
                ("HF_TOKEN".into(), "abc=def".into()),
                ("UPLOAD_DIR".into(), "/data".into())
            ]
        );
        assert!(parse_env_file("HOMEBASED_HOME=/x").is_err());
        assert!(parse_env_file("1BAD=x").is_err());
        assert!(parse_env_file("novalue").is_err());
    }

    #[test]
    fn join_tokens_hash_stably_and_hide_in_debug() {
        let token = JoinToken::generate().unwrap();
        assert_eq!(token.expose().len(), 64);
        assert_eq!(token.hash(), JoinToken::new(token.expose().into()).hash());
        assert_eq!(format!("{token:?}"), "JoinToken(..)");
    }
}
