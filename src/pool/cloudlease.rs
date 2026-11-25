//! Client of the cloudlease API that creates, renews, and deletes temporary
//! machines
//!
//! The coordinator calls it with the app's client token; a temporary machine
//! calls it with its own lease token, which can renew or release only that
//! lease. The contract is the "Temporary machines contract" section of the
//! cloudlease design note, mirrored in `docs/temporary-machines.md`.
//! Requests go through the system `curl`, so callers run them off the async
//! runtime

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::boot::BootPayload;
use super::{LeaseId, MachineClass, PoolName, PoolSettings, ReleaseReason, RunId, SlotId};
use crate::curl::{CurlMethod, CurlRequest, send};
use crate::machine::MachineName;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A cloudlease call that did not succeed
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CloudleaseError {
    /// cloudlease answered with a definite refusal; retrying the same call will not help
    #[error("cloudlease refused ({status} {code}): {message}")]
    Refused {
        /// HTTP status
        status: u16,
        /// Error code
        code: String,
        /// Error message
        message: String,
    },
    /// The call may succeed later: transport failure, timeout, or a retryable answer
    #[error("cloudlease unavailable: {message}")]
    Unavailable {
        /// What went wrong
        message: String,
    },
}

impl CloudleaseError {
    /// Error code of a refusal
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Refused { code, .. } => Some(code),
            Self::Unavailable { .. } => None,
        }
    }

    /// Whether the refusal says the run or lease no longer exists or has ended
    #[must_use]
    pub fn is_gone(&self) -> bool {
        matches!(
            self.code(),
            Some("run_not_found" | "lease_not_found" | "lease_ended")
        )
    }

    /// Whether the refusal only means no more machines this round
    #[must_use]
    pub fn is_capacity(&self) -> bool {
        matches!(self.code(), Some("run_full" | "request_conflict"))
    }
}

/// Where a run is in its life
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    /// It takes new machines
    Open,
    /// It takes no new machines and its leases are ending
    Releasing,
    /// Every lease of it is released
    Released,
}

/// Where a lease is in its life
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    /// The provider is creating the machine
    Provisioning,
    /// The machine exists
    Running,
    /// Its resources are being deleted
    Releasing,
    /// Every resource of it was confirmed absent
    Released,
}

/// Who started a release
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseBy {
    /// The app, with its client token
    Client,
    /// The machine, with its lease token
    Machine,
    /// cloudlease itself: a deadline, a lapsed heartbeat, or a cap
    Cloudlease,
}

/// A started release
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRelease {
    /// Why, as cloudlease names it
    pub reason: String,
    /// Who started it
    pub by: ReleaseBy,
    /// When
    pub at: DateTime<Utc>,
}

/// One machine's lease
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// Its id
    pub lease_id: LeaseId,
    /// Its run
    pub run_id: RunId,
    /// The create's `request_id`, the coordinator's slot
    pub request_id: Uuid,
    /// The machine's hostname
    pub hostname: String,
    /// Where it is in its life
    pub state: LeaseState,
    /// When cloudlease created it
    pub created_at: DateTime<Utc>,
    /// cloudlease deletes the machine at this time whatever else happens
    pub hard_deadline_at: DateTime<Utc>,
    /// cloudlease deletes the machine at this time unless it renews
    pub heartbeat_deadline_at: DateTime<Utc>,
    /// Its release, once started
    #[serde(default)]
    pub release: Option<LeaseRelease>,
    /// When every resource was confirmed absent
    #[serde(default)]
    pub gone_at: Option<DateTime<Utc>>,
}

impl Lease {
    /// Why it was released, when it is releasing or released
    #[must_use]
    pub fn release_reason(&self) -> Option<ReleaseReason> {
        self.release
            .as_ref()
            .map(|release| ReleaseReason::from_wire(&release.reason))
    }
}

/// One pool's run
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    /// Its id
    pub run_id: RunId,
    /// Its name, the pool's
    pub name: String,
    /// Machine class of every lease
    pub class: MachineClass,
    /// Most live leases at once
    pub max_machines: u32,
    /// No lease outlives this
    pub deadline_at: DateTime<Utc>,
    /// Where it is in its life
    pub state: RunState,
    /// When it was created
    pub created_at: DateTime<Utc>,
    /// Every lease of the run
    #[serde(default)]
    pub machines: Vec<Lease>,
}

/// Answer to a renew
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Renewed {
    /// The lease ends at this time unless renewed again
    pub heartbeat_deadline_at: DateTime<Utc>,
    /// The lease ends at this time whatever happens
    pub hard_deadline_at: DateTime<Utc>,
    /// An idle machine may stay until this time without adding to its bill,
    /// as the rest of a provider hour already paid for; absent when holding
    /// it gains nothing, as on a provider that bills by the second
    #[serde(default)]
    pub idle_hold_until: Option<DateTime<Utc>>,
}

/// Why a machine releases its own lease
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelfRelease {
    /// It had no work for its idle limit
    Idle,
    /// Its work is done
    Done,
}

/// The coordinator's connection to cloudlease, with the app's client token
#[derive(Debug, Clone)]
pub struct CloudleaseClient {
    base: String,
    token_file: PathBuf,
}

impl CloudleaseClient {
    /// A client of the API at `base`, reading its token from `token_file` for each call
    #[must_use]
    pub fn new(base: &str, token_file: PathBuf) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            token_file,
        }
    }

    /// Create a pool's run, or read it back when `request_id` was used before
    ///
    /// A retry sends the same settings, spend cap included, as cloudlease
    /// matches a retried create by its whole body
    pub fn create_run(
        &self,
        request_id: Uuid,
        pool: &PoolName,
        settings: &PoolSettings,
    ) -> Result<Run, CloudleaseError> {
        let mut body = json!({
            "request_id": request_id,
            "name": pool,
            "class": settings.machine_class,
            "max_machines": settings.max_machines,
            "deadline_at": settings.deadline_at,
        });
        // absent means cloudlease's own per-run cap
        if let Some(cap) = settings.spend_cap_cents {
            body["spend_cap_cents"] = json!(cap);
        }
        self.call(CurlMethod::Post, "/v1/runs", Some(&body))
    }

    /// Read a run with every lease
    pub fn run(&self, run: &RunId) -> Result<Run, CloudleaseError> {
        self.call(CurlMethod::Get, &format!("/v1/runs/{run}"), None)
    }

    /// Stop a run taking machines and release all of its leases
    pub fn release_run(&self, run: &RunId) -> Result<(), CloudleaseError> {
        let body = json!({ "reason": "requested" });
        self.call::<Value>(
            CurlMethod::Post,
            &format!("/v1/runs/{run}/release"),
            Some(&body),
        )
        .map(drop)
    }

    /// Ask for one machine; a retry with the same slot returns the same lease
    pub fn create_machine(
        &self,
        run: &RunId,
        slot: SlotId,
        hostname: &MachineName,
        boot: &BootPayload,
    ) -> Result<Lease, CloudleaseError> {
        let body = json!({
            "request_id": slot,
            "hostname": hostname,
            "boot": boot,
        });
        self.call(
            CurlMethod::Post,
            &format!("/v1/runs/{run}/machines"),
            Some(&body),
        )
    }

    /// Release one lease
    pub fn release_lease(&self, lease: &LeaseId) -> Result<(), CloudleaseError> {
        let body = json!({ "reason": "requested" });
        self.call::<Value>(
            CurlMethod::Post,
            &format!("/v1/leases/{lease}/release"),
            Some(&body),
        )
        .map(drop)
    }

    fn call<T: DeserializeOwned>(
        &self,
        method: CurlMethod,
        path: &str,
        body: Option<&Value>,
    ) -> Result<T, CloudleaseError> {
        let token = read_token(&self.token_file)?;
        request(method, &format!("{}{path}", self.base), &token, body)
    }
}

/// A machine's lease as cloudlease wrote it to `lease.json` at boot
#[derive(Clone, Deserialize)]
pub struct LeaseCredential {
    /// The lease
    pub lease_id: LeaseId,
    /// Token that renews or releases only this lease
    token: String,
    /// Base URL of the cloudlease API
    pub api_base: String,
    /// cloudlease deletes the machine at this time whatever happens
    pub hard_deadline_at: DateTime<Utc>,
    /// cloudlease deletes the machine this long after its last renew
    heartbeat_ttl_secs: u64,
}

impl std::fmt::Debug for LeaseCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseCredential")
            .field("lease_id", &self.lease_id)
            .field("api_base", &self.api_base)
            .field("hard_deadline_at", &self.hard_deadline_at)
            .field("heartbeat_ttl_secs", &self.heartbeat_ttl_secs)
            .finish_non_exhaustive()
    }
}

impl LeaseCredential {
    /// Read `lease.json`
    pub fn read(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        serde_json::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display()))
    }

    /// Time between renewals: `configured`, but never slower than a quarter
    /// of the lease's heartbeat window, so a failed renew has rounds to spare
    #[must_use]
    pub fn renew_interval(&self, configured: Duration) -> Duration {
        let quarter = Duration::from_secs(self.heartbeat_ttl_secs) / 4;
        if quarter.is_zero() {
            return configured;
        }
        configured.min(quarter)
    }

    /// Move the heartbeat deadline; `410 lease_ended` means the machine is going away
    pub fn renew(&self) -> Result<Renewed, CloudleaseError> {
        request(
            CurlMethod::Post,
            &self.url("renew"),
            &self.token,
            Some(&json!({})),
        )
    }

    /// Release this lease
    pub fn release(&self, reason: SelfRelease) -> Result<(), CloudleaseError> {
        request::<Value>(
            CurlMethod::Post,
            &self.url("release"),
            &self.token,
            Some(&json!({ "reason": reason })),
        )
        .map(drop)
    }

    fn url(&self, action: &str) -> String {
        format!(
            "{}/v1/leases/{}/{action}",
            self.api_base.trim_end_matches('/'),
            self.lease_id
        )
    }
}

fn read_token(path: &Path) -> Result<String, CloudleaseError> {
    // a missing or unreadable token file is retried, since it may be mid-rotation;
    // only cloudlease's own answer ends a pool
    let text = std::fs::read_to_string(path).map_err(|error| CloudleaseError::Unavailable {
        message: format!("read {}: {error}", path.display()),
    })?;
    Ok(text.trim().to_owned())
}

#[derive(Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    code: String,
    message: String,
    #[serde(default)]
    retryable: bool,
}

fn request<T: DeserializeOwned>(
    method: CurlMethod,
    url: &str,
    token: &str,
    body: Option<&Value>,
) -> Result<T, CloudleaseError> {
    let response = send(&CurlRequest {
        method,
        url,
        bearer: Some(token),
        json_body: body,
        timeout: REQUEST_TIMEOUT,
    })
    .map_err(|message| CloudleaseError::Unavailable { message })?;
    if (200..300).contains(&response.status) {
        return serde_json::from_str(&response.body).map_err(|error| {
            CloudleaseError::Unavailable {
                message: format!("unreadable answer from {url}: {error}"),
            }
        });
    }
    let detail = serde_json::from_str::<ErrorBody>(&response.body).ok();
    let retryable = response.status >= 500
        || response.status == 408
        || detail.as_ref().is_some_and(|body| body.error.retryable);
    let (code, message) = detail.map_or_else(
        || {
            (
                "http_error".to_owned(),
                response.body.chars().take(300).collect(),
            )
        },
        |body| (body.error.code, body.error.message),
    );
    if retryable {
        return Err(CloudleaseError::Unavailable {
            message: format!("{} {code}: {message}", response.status),
        });
    }
    Err(CloudleaseError::Refused {
        status: response.status,
        code,
        message,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::LeaseCredential;

    fn lease(ttl: u64) -> LeaseCredential {
        serde_json::from_value(serde_json::json!({
            "lease_id": "run_1-1",
            "token": "cll_secret",
            "api_base": "https://cloudlease.example",
            "hard_deadline_at": "2026-11-25T02:00:00Z",
            "heartbeat_ttl_secs": ttl,
        }))
        .unwrap()
    }

    #[test]
    fn renewals_come_at_least_four_times_per_heartbeat_window() {
        let configured = Duration::from_secs(30);
        // the default 3-minute window leaves the configured 30 s alone
        assert_eq!(lease(180).renew_interval(configured), configured);
        // the shortest window cloudlease allows forces 15 s
        assert_eq!(
            lease(60).renew_interval(configured),
            Duration::from_secs(15)
        );
    }
}
