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
pub mod bundle;
pub mod cloudlease;
pub mod error;
pub mod renewal;

use std::fmt;
use std::num::NonZeroU32;
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

/// How long before cloudlease's boot grace ends a machine that has not
/// joined is released, at most; a grace shorter than four of these keeps a
/// quarter of itself instead
///
/// A machine renews only once it joined, so cloudlease deletes it at the end
/// of the grace anyway; releasing it first records `boot_timeout` as the
/// reason rather than `heartbeat_lapsed`
pub const BOOT_GRACE_MARGIN: Duration = Duration::from_secs(60);

/// cloudlease's boot grace when no lease of the pool has shown its own yet:
/// cloudlease's default, 8 minutes
pub const ASSUMED_BOOT_GRACE: Duration = Duration::from_secs(8 * 60);

/// Free disk, in GiB, below which a pool machine admits no CI run, by default
///
/// Lower than a Fleet machine's 10 GiB: a pool machine is fresh and runs only
/// its pool's jobs, so its class's disk decides what fits, and a small
/// machine must still take work
pub const DEFAULT_MIN_FREE_DISK_GIB: u64 = 1;

/// Time from a lease's create to the release of its machine if it has not
/// joined, for a boot grace of `grace`
#[must_use]
pub fn boot_timeout_within(grace: Duration) -> Duration {
    grace.saturating_sub(BOOT_GRACE_MARGIN.min(grace / 4))
}

/// Times a job whose machine was interrupted runs again on another machine
/// of its pool, by default
pub const DEFAULT_JOB_RETRIES: u32 = 2;

/// Most times a pool may run a job again after its machine was interrupted
pub const MAX_JOB_RETRIES: u32 = 10;

/// Machines in a row the provider could not create before a pool stops
/// asking, so a broken class or region never turns into a create loop
pub const MAX_CREATE_FAILURES: u32 = 3;

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
    /// A machine that has not joined this long after its create is
    /// released; without it, shortly before cloudlease's boot grace ends,
    /// which each lease shows in its first `heartbeat_deadline_at`
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub boot_timeout: Option<Duration>,
    /// Free disk, in GiB, below which a machine of the pool admits no CI run
    pub min_free_disk_gib: u64,
    /// File on the coordinator of `NAME=value` lines each machine's runs
    /// get, read again at each join; never stored or sent to cloudlease
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<PathBuf>,
    /// Most the pool's run may spend, in US cents; cloudlease's own per-run
    /// cap when absent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spend_cap_cents: Option<NonZeroU32>,
    /// Times a job whose machine was interrupted runs again on another
    /// machine of the pool
    pub job_retries: u32,
}

impl PoolSettings {
    /// When a machine of `lease` that has not joined is released
    #[must_use]
    pub fn boot_released_at(&self, lease: &MachineLease) -> DateTime<Utc> {
        let timeout = self
            .boot_timeout
            .unwrap_or_else(|| boot_timeout_within(lease.boot_grace()));
        lease.created_at + chrono::Duration::from_std(timeout).unwrap_or_default()
    }
}

/// Why cloudlease ended a pool's run that the coordinator did not release
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEndCause {
    /// Its machines reached the run's spend cap
    SpendCap,
    /// Someone else released it, such as an operator of cloudlease
    Released,
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
    /// The provider could not create this many machines in a row
    CreateFailed {
        /// Failed creates in a row
        failures: u32,
    },
    /// cloudlease ended the pool's run before its deadline, without this
    /// coordinator asking
    RunEnded {
        /// Why, as far as the run's leases tell
        cause: RunEndCause,
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
            Self::CreateFailed { .. } => "create_failed",
            Self::RunEnded { .. } => "run_ended",
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
            Self::CreateFailed { failures } => {
                format!("the provider could not create {failures} machines of pool {pool} in a row")
            }
            Self::RunEnded {
                cause: RunEndCause::SpendCap,
            } => format!("cloudlease ended pool {pool}'s run: it reached its spend cap"),
            Self::RunEnded {
                cause: RunEndCause::Released,
            } => format!(
                "cloudlease ended pool {pool}'s run before its deadline, without its coordinator asking"
            ),
        }
    }
}

/// Where a pool is in its life
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PoolState {
    /// It asks for machines as its jobs need them
    Open,
    /// It asks for no more machines and takes no new job; its machines
    /// finish the jobs they run and release themselves once idle
    Stopped {
        /// Why
        #[serde(flatten)]
        end: PoolEnd,
    },
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
            Self::Stopped { .. } => "stopped",
            Self::Releasing { .. } => "releasing",
            Self::Released { .. } => "released",
        }
    }

    /// How far along its life it is; a pool never moves back
    #[must_use]
    pub fn stage(&self) -> u8 {
        match self {
            Self::Open => 0,
            Self::Stopped { .. } => 1,
            Self::Releasing { .. } => 2,
            Self::Released { .. } => 3,
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
    /// Machines in a row the provider could not create, since the last
    /// create it took
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_failures: Option<CreateFailures>,
}

impl Pool {
    /// Count a machine the provider could not create; the pool stops at
    /// [`MAX_CREATE_FAILURES`] in a row
    pub fn create_failed(&mut self, at: DateTime<Utc>) {
        let count = self
            .create_failures
            .map_or(1, |failures| failures.count + 1);
        self.create_failures = Some(CreateFailures { count, last_at: at });
        if count >= MAX_CREATE_FAILURES {
            self.stop(PoolEnd::CreateFailed { failures: count });
        }
    }

    /// Ask for no more machines and take no new job, for `end`, when still open
    pub fn stop(&mut self, end: PoolEnd) {
        if matches!(self.state, PoolState::Open) {
            self.state = PoolState::Stopped { end };
        }
    }

    /// Whether a held CI job of the pool's class that its origin took at
    /// `submitted_at` is the pool's to end if it never got a machine
    ///
    /// A pool owns its class until it is released. A job taken after its
    /// release was admitted for a Fleet machine that offers the class, so it
    /// is never the released pool's to end. A route with no submit time
    /// predates pools
    #[must_use]
    pub fn owns(&self, submitted_at: Option<DateTime<Utc>>) -> bool {
        match (&self.state, submitted_at) {
            (PoolState::Released { at, .. }, Some(submitted_at)) => submitted_at <= *at,
            _ => true,
        }
    }

    /// The provider took a create, so earlier failures were not the class's fault
    pub fn create_succeeded(&mut self) {
        self.create_failures = None;
    }

    /// Whether a new machine may be asked for at `now`: after a failed
    /// create the pool waits `backoff`, doubled for each failure in a row
    #[must_use]
    pub fn creates_allowed(&self, now: DateTime<Utc>, backoff: Duration) -> bool {
        let Some(failures) = self.create_failures else {
            return true;
        };
        let doublings = failures.count.saturating_sub(1).min(8);
        let wait = backoff.saturating_mul(1 << doublings);
        chrono::Duration::from_std(wait)
            .ok()
            .and_then(|wait| failures.last_at.checked_add_signed(wait))
            .is_some_and(|at| now >= at)
    }
}

/// Machines in a row the provider could not create
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateFailures {
    /// How many
    pub count: u32,
    /// When the last one was seen
    pub last_at: DateTime<Utc>,
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
    /// The provider took it back, such as an AWS Spot interruption, or it
    /// disappeared some other way
    Interrupted,
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

    /// Whether a job that lost its machine for this reason runs again on
    /// another machine of the pool
    ///
    /// Only an interruption does: the job did nothing wrong and another
    /// machine can finish it. A lapsed heartbeat does not, since it usually
    /// means every run on the machine stalled, which a rerun would repeat
    #[must_use]
    pub fn reruns_job(&self) -> bool {
        matches!(self, Self::Interrupted)
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
    /// When cloudlease's boot grace ends: the lease's first
    /// `heartbeat_deadline_at`, before the machine ever renewed
    pub boot_deadline_at: DateTime<Utc>,
}

impl MachineLease {
    /// cloudlease's boot grace for this lease
    #[must_use]
    pub fn boot_grace(&self) -> Duration {
        (self.boot_deadline_at - self.created_at)
            .to_std()
            .unwrap_or_default()
    }
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

/// Which jobs a pool's machines can still run to their kill limit, as of one
/// moment
///
/// cloudlease deletes every machine of the pool by the pool's deadline, or
/// sooner when the machine class has a shorter lifetime, and placement sends
/// a job only to a machine that outlives the job's kill limit. Time only
/// takes reach away, so a job out of reach now never runs on the pool
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolReach {
    now: DateTime<Utc>,
    /// The latest hard deadline of a machine that is booting or joined
    live_until: Option<DateTime<Utc>>,
    /// A machine asked for now: when it has joined at the latest, and when
    /// cloudlease deletes it; `None` once the pool asks for no machines
    new_machine: Option<NewMachine>,
}

/// Free or coming slots of the pool's class on one machine the pool has
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComingSlots {
    /// When cloudlease deletes the machine; `None` while its create has no
    /// lease yet
    pub ends_at: Option<DateTime<Utc>>,
    /// Slots a waiting job could take on it
    pub free: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NewMachine {
    joined_by: DateTime<Utc>,
    ends_at: DateTime<Utc>,
}

impl PoolReach {
    /// Reach of `pool` with `machines`, every one it has had, at `now`
    #[must_use]
    pub fn new(pool: &Pool, machines: &[PoolMachine], now: DateTime<Utc>) -> Self {
        Self::without(pool, machines, None, now)
    }

    /// Reach of `pool` once the machine in slot `lost` is gone: it runs
    /// nothing more, but its lease still shows the class's boot grace and
    /// lifetime, which may be the only lease that does
    fn without(
        pool: &Pool,
        machines: &[PoolMachine],
        lost: Option<SlotId>,
        now: DateTime<Utc>,
    ) -> Self {
        let live_until = machines
            .iter()
            .filter(|machine| Some(machine.slot) != lost)
            .filter_map(|machine| match &machine.state {
                MachineState::Booting { lease } | MachineState::Joined { lease, .. } => {
                    Some(lease.hard_deadline_at)
                }
                _ => None,
            })
            .max();
        let new_machine = matches!(pool.state, PoolState::Open).then(|| {
            let deadline = pool.settings.deadline_at;
            let leases: Vec<&MachineLease> = machines
                .iter()
                .filter_map(|machine| machine.state.lease())
                .collect();
            // every lease of the pool has the same grace and class lifetime,
            // so the newest one tells
            let shown_grace = leases
                .iter()
                .max_by_key(|lease| lease.created_at)
                .map(|lease| lease.boot_grace());
            // cloudlease deletes a machine that has not joined by the end of
            // its grace, so a longer boot timeout never delays a join
            // further; before a lease showed the grace, a set boot timeout
            // is the only bound, and without one cloudlease's default
            let join_within = match (pool.settings.boot_timeout, shown_grace) {
                (Some(timeout), Some(grace)) => timeout.min(grace),
                (Some(timeout), None) => timeout,
                (None, grace) => boot_timeout_within(grace.unwrap_or(ASSUMED_BOOT_GRACE)),
            };
            // a lease that ends before the pool's deadline shows the class's
            // own lifetime, which every later machine gets too
            let class_lifetime = leases
                .iter()
                .filter(|lease| lease.hard_deadline_at < deadline)
                .map(|lease| lease.hard_deadline_at - lease.created_at)
                .min();
            NewMachine {
                joined_by: now + chrono::Duration::from_std(join_within).unwrap_or_default(),
                ends_at: class_lifetime.map_or(deadline, |lifetime| (now + lifetime).min(deadline)),
            }
        });
        Self {
            now,
            live_until,
            new_machine,
        }
    }

    /// Whether a job with kill limit `kill` can still run on some machine of
    /// the pool: one it has, if a slot frees in time, or one it may ask for
    #[must_use]
    pub fn can_run(&self, kill: Duration) -> bool {
        let fits_live = self
            .live_until
            .is_some_and(|until| ends_by(self.now, kill, until));
        fits_live || self.new_machine_can_run(kill)
    }

    /// Whether a machine asked for now could run a job with kill limit
    /// `kill` once it joined
    #[must_use]
    pub fn new_machine_can_run(&self, kill: Duration) -> bool {
        self.new_machine
            .is_some_and(|machine| ends_by(machine.joined_by, kill, machine.ends_at))
    }

    /// How many jobs, by kill limit, no slot in `coming` can run: each job,
    /// longest kill limit first, takes a free slot on the machine that ends
    /// soonest among those that outlive it, which leaves the later-ending
    /// slots to the jobs that need them
    #[must_use]
    pub fn jobs_without_slot(&self, mut kills: Vec<Duration>, mut coming: Vec<ComingSlots>) -> u32 {
        kills.sort_unstable_by(|left, right| right.cmp(left));
        // a machine whose create is still in flight ends like a new one,
        // and every job asked about fits a new machine, so it sorts last
        coming.sort_by_key(|slots| slots.ends_at.unwrap_or(DateTime::<Utc>::MAX_UTC));
        let mut without = 0_u32;
        for kill in kills {
            let slot = coming.iter_mut().find(|slots| {
                slots.free > 0 && slots.ends_at.is_none_or(|end| ends_by(self.now, kill, end))
            });
            match slot {
                Some(slots) => slots.free -= 1,
                None => without += 1,
            }
        }
        without
    }

    /// The longest kill limit a job could have now and still run, for error
    /// messages
    #[must_use]
    pub fn longest_kill(&self) -> Duration {
        let live = self.live_until.map(|until| until - self.now);
        let new = self
            .new_machine
            .map(|machine| machine.ends_at - machine.joined_by);
        live.max(new)
            .and_then(|span| span.to_std().ok())
            .unwrap_or_default()
    }
}

/// Whether a run started at `start` and killed after `kill` ends by `until`
fn ends_by(start: DateTime<Utc>, kill: Duration, until: DateTime<Utc>) -> bool {
    chrono::Duration::from_std(kill)
        .ok()
        .and_then(|kill| start.checked_add_signed(kill))
        .is_some_and(|end| end <= until)
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
    /// Which run of the job ended with the machine: 1 for its first, more
    /// after its earlier machines were interrupted
    pub attempt: u32,
    /// Why a job whose machine was interrupted was not run again
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_rerun: Option<NotRerun>,
}

/// Why a job whose machine was interrupted ended instead of running again
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cause", rename_all = "snake_case")]
pub enum NotRerun {
    /// It ran as many times as its pool allows
    RetriesExhausted {
        /// Runs the pool allows each job: its retries and the first run
        max_attempts: u32,
    },
    /// The pool takes no more jobs
    PoolEnded,
    /// No machine of the pool could run it to its kill limit before the
    /// pool's deadline
    OutOfReach,
    /// A move pinned it to the machine that was lost
    Pinned,
    /// Someone asked to cancel it
    Cancelled,
}

/// Carried by the `JOB_PREEMPTED` its origin writes when a job's temporary
/// machine was lost and the job is queued for another machine of its pool
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineLost {
    /// The pool
    pub pool: PoolName,
    /// The machine that was lost
    pub machine: MachineName,
    /// Why
    pub reason: ReleaseReason,
    /// The run now queued: 2 for the first rerun
    pub attempt: u32,
    /// Runs the pool allows each job: its retries and the first run
    pub max_attempts: u32,
}

/// Whether a job that lost its machine runs again, and as which attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rerun {
    /// Queue it for another machine of the pool as this attempt
    Queue {
        /// The attempt now queued
        attempt: u32,
    },
    /// End it with its machine; `None` when the reason is never rerun
    End(Option<NotRerun>),
}

/// What decides whether a job that lost its machine runs again
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RerunPolicy {
    /// Runs the pool allows each job: its retries and the first run
    pub max_attempts: u32,
    /// What the pool's machines can still run; `None` once the pool takes
    /// no new jobs
    pub reach: Option<PoolReach>,
}

impl RerunPolicy {
    /// The policy of `pool` at `now`, with `machines` every one it has had,
    /// once the machine in slot `lost` is gone
    #[must_use]
    pub fn new(pool: &Pool, machines: &[PoolMachine], lost: SlotId, now: DateTime<Utc>) -> Self {
        Self {
            max_attempts: pool.settings.job_retries.saturating_add(1),
            reach: matches!(pool.state, PoolState::Open)
                .then(|| PoolReach::without(pool, machines, Some(lost), now)),
        }
    }

    /// Decide for a job on run `attempt` with kill limit `kill` whose
    /// machine was lost for `reason`; `pinned` when a move tied it to that
    /// machine
    #[must_use]
    pub fn decide(
        &self,
        reason: &ReleaseReason,
        attempt: u32,
        kill: Duration,
        pinned: bool,
    ) -> Rerun {
        if !reason.reruns_job() {
            return Rerun::End(None);
        }
        if attempt >= self.max_attempts {
            return Rerun::End(Some(NotRerun::RetriesExhausted {
                max_attempts: self.max_attempts,
            }));
        }
        if pinned {
            return Rerun::End(Some(NotRerun::Pinned));
        }
        let Some(reach) = self.reach else {
            return Rerun::End(Some(NotRerun::PoolEnded));
        };
        // the deadline bounds every machine, and a spend cap refuses the
        // replacement's create, which ends the pool and the queued job with it
        if !reach.can_run(kill) {
            return Rerun::End(Some(NotRerun::OutOfReach));
        }
        Rerun::Queue {
            attempt: attempt + 1,
        }
    }
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

    fn test_pool(deadline_at: DateTime<Utc>, boot_timeout: Option<Duration>) -> Pool {
        Pool {
            name: "afv".parse().unwrap(),
            settings: PoolSettings {
                machine_class: "do-c16".parse().unwrap(),
                max_machines: 4,
                deadline_at,
                slots: SlotCapacity::default(),
                idle_limit: DEFAULT_IDLE_LIMIT,
                stall_after: DEFAULT_STALL_AFTER,
                boot_timeout,
                min_free_disk_gib: DEFAULT_MIN_FREE_DISK_GIB,
                env_file: None,
                spend_cap_cents: None,
                job_retries: DEFAULT_JOB_RETRIES,
            },
            run_request: Uuid::now_v7(),
            run: None,
            state: PoolState::Open,
            created_at: deadline_at - chrono::Duration::hours(6),
            last_error: None,
            create_failures: None,
        }
    }

    fn booting(pool: &Pool, created_at: DateTime<Utc>, grace: i64, life: i64) -> PoolMachine {
        let slot = SlotId::new();
        PoolMachine {
            slot,
            pool: pool.name.clone(),
            name: pool.name.machine_name(slot),
            join_token: JoinToken::new("t".into()).hash(),
            requested_at: created_at,
            state: MachineState::Booting {
                lease: MachineLease {
                    lease: LeaseId::new("lease"),
                    hard_deadline_at: created_at + chrono::Duration::minutes(life),
                    created_at,
                    boot_deadline_at: created_at + chrono::Duration::minutes(grace),
                },
            },
        }
    }

    const MINUTE: Duration = Duration::from_secs(60);

    #[test]
    fn reach_counts_a_new_machines_boot_and_learns_grace_and_class_lifetime() {
        let now = Utc::now();
        let pool = test_pool(now + chrono::Duration::hours(1), None);

        // before any lease: cloudlease's default 8 minute grace, released at 7
        let reach = PoolReach::new(&pool, &[], now);
        assert!(reach.can_run(53 * MINUTE));
        assert!(!reach.can_run(54 * MINUTE));
        assert_eq!(reach.longest_kill(), 53 * MINUTE);

        // a phoenixNAP lease shows a 20 minute grace; it is released at 19
        let mut released = booting(&pool, now - chrono::Duration::hours(1), 20, 600);
        released.state = MachineState::Released {
            lease: released.state.lease().cloned(),
            joined: None,
            reason: ReleaseReason::Idle,
            gone_at: now,
        };
        let reach = PoolReach::new(&pool, std::slice::from_ref(&released), now);
        assert!(reach.new_machine_can_run(41 * MINUTE));
        assert!(!reach.new_machine_can_run(42 * MINUTE));

        // a class whose machines live 30 minutes caps every new machine, but
        // a booting machine still offers what is left of its own life
        let short = booting(&pool, now - chrono::Duration::minutes(5), 8, 30);
        let reach = PoolReach::new(&pool, &[short], now);
        assert!(!reach.new_machine_can_run(24 * MINUTE));
        assert!(reach.new_machine_can_run(23 * MINUTE));
        assert!(reach.can_run(25 * MINUTE));
        assert!(!reach.can_run(26 * MINUTE));

        // a stopped pool asks for no machine, so only live ones count
        let mut stopped = test_pool(now + chrono::Duration::hours(1), Some(MINUTE));
        stopped.stop(PoolEnd::Requested);
        assert!(!PoolReach::new(&stopped, &[], now).can_run(MINUTE));
    }

    #[test]
    fn a_boot_timeout_past_the_grace_does_not_shrink_reach() {
        let now = Utc::now();
        // cloudlease deletes an unjoined machine at its 8 minute grace, so a
        // 55 minute boot timeout still leaves a new machine 52 minutes
        let pool = test_pool(now + chrono::Duration::hours(1), Some(55 * MINUTE));
        let lease = booting(&pool, now - chrono::Duration::minutes(1), 8, 120);
        let reach = PoolReach::new(&pool, std::slice::from_ref(&lease), now);
        assert!(reach.new_machine_can_run(52 * MINUTE));
        assert!(!reach.new_machine_can_run(53 * MINUTE));

        // before any lease showed the grace, a set boot timeout is the bound,
        // as for a phoenixNAP pool whose grace is 20 minutes
        let pool = test_pool(now + chrono::Duration::minutes(30), Some(19 * MINUTE));
        let reach = PoolReach::new(&pool, &[], now);
        assert!(reach.new_machine_can_run(11 * MINUTE));
        assert!(!reach.new_machine_can_run(12 * MINUTE));
    }

    #[test]
    fn slots_on_machines_that_end_too_soon_leave_jobs_without_a_slot() {
        let now = Utc::now();
        let pool = test_pool(now + chrono::Duration::minutes(25), None);
        let reach = PoolReach::new(&pool, &[], now);
        let soon = ComingSlots {
            ends_at: Some(now + chrono::Duration::minutes(2)),
            free: 1,
        };
        let late = ComingSlots {
            ends_at: Some(now + chrono::Duration::minutes(25)),
            free: 1,
        };
        let in_flight = ComingSlots {
            ends_at: None,
            free: 1,
        };
        // a free slot that ends in 2 minutes takes no 17 minute job
        assert_eq!(reach.jobs_without_slot(vec![17 * MINUTE], vec![soon]), 1);
        // the short job takes the early slot, the long one the late slot
        assert_eq!(
            reach.jobs_without_slot(vec![MINUTE, 17 * MINUTE], vec![late, soon]),
            0
        );
        assert_eq!(
            reach.jobs_without_slot(vec![17 * MINUTE, 17 * MINUTE], vec![soon, in_flight]),
            1
        );
    }

    #[test]
    fn boot_timeouts_stop_short_of_the_grace_unless_set() {
        assert_eq!(boot_timeout_within(8 * MINUTE), 7 * MINUTE);
        assert_eq!(boot_timeout_within(20 * MINUTE), 19 * MINUTE);
        assert_eq!(boot_timeout_within(MINUTE), Duration::from_secs(45));
        let now = Utc::now();
        let pool = test_pool(now + chrono::Duration::hours(1), None);
        let machine = booting(&pool, now, 20, 60);
        let lease = machine.state.lease().unwrap();
        assert_eq!(
            pool.settings.boot_released_at(lease),
            now + chrono::Duration::minutes(19)
        );
        let set = test_pool(now + chrono::Duration::hours(1), Some(2 * MINUTE));
        assert_eq!(
            set.settings.boot_released_at(lease),
            now + chrono::Duration::minutes(2)
        );
    }

    #[test]
    fn only_an_interruption_reruns_and_only_within_the_limit_and_reach() {
        let now = Utc::now();
        let pool = test_pool(now + chrono::Duration::hours(1), None);
        let policy = RerunPolicy::new(&pool, &[], SlotId::new(), now);
        assert_eq!(policy.max_attempts, 3);
        let interrupted = ReleaseReason::Interrupted;
        assert_eq!(
            policy.decide(&interrupted, 1, 10 * MINUTE, false),
            Rerun::Queue { attempt: 2 }
        );
        assert_eq!(
            policy.decide(&interrupted, 3, 10 * MINUTE, false),
            Rerun::End(Some(NotRerun::RetriesExhausted { max_attempts: 3 }))
        );
        // a stall would repeat, so a lapsed heartbeat is never rerun
        assert_eq!(
            policy.decide(&ReleaseReason::HeartbeatLapsed, 1, 10 * MINUTE, false),
            Rerun::End(None)
        );
        assert_eq!(
            policy.decide(&interrupted, 1, 10 * MINUTE, true),
            Rerun::End(Some(NotRerun::Pinned))
        );
        // a new machine joins within 7 minutes, so 54 minutes no longer fit
        assert_eq!(
            policy.decide(&interrupted, 1, 54 * MINUTE, false),
            Rerun::End(Some(NotRerun::OutOfReach))
        );

        let mut stopped = pool.clone();
        stopped.stop(PoolEnd::Requested);
        assert_eq!(
            RerunPolicy::new(&stopped, &[], SlotId::new(), now).decide(
                &interrupted,
                1,
                MINUTE,
                false
            ),
            Rerun::End(Some(NotRerun::PoolEnded))
        );
        assert_eq!(
            ReleaseReason::from_wire("interrupted"),
            ReleaseReason::Interrupted
        );

        // the lost machine's lease may be the only one showing a 4 minute
        // grace: a replacement then joins within 3 minutes, so a 10 minute
        // job fits in the 15 left, though the assumed 7 minutes would not
        let closing = test_pool(now + chrono::Duration::minutes(15), None);
        let lost = booting(&closing, now - chrono::Duration::minutes(5), 4, 60);
        let policy = RerunPolicy::new(&closing, std::slice::from_ref(&lost), lost.slot, now);
        assert_eq!(
            policy.decide(&interrupted, 1, 10 * MINUTE, false),
            Rerun::Queue { attempt: 2 }
        );
        // it runs nothing more, so a job only it could outlive is out of reach
        let stopped_closing = {
            let mut pool = closing.clone();
            pool.stop(PoolEnd::Requested);
            pool
        };
        assert!(
            PoolReach::new(&stopped_closing, std::slice::from_ref(&lost), now).can_run(10 * MINUTE)
        );
        assert!(
            !PoolReach::without(
                &stopped_closing,
                std::slice::from_ref(&lost),
                Some(lost.slot),
                now
            )
            .can_run(10 * MINUTE)
        );
    }

    #[test]
    fn join_tokens_hash_stably_and_hide_in_debug() {
        let token = JoinToken::generate().unwrap();
        assert_eq!(token.expose().len(), 64);
        assert_eq!(token.hash(), JoinToken::new(token.expose().into()).hash());
        assert_eq!(format!("{token:?}"), "JoinToken(..)");
    }
}
