//! The `JOB_GPU_IDLE` payload and the agent controls that silence it

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::process::ProcessSnapshot;
use crate::domain::TaskId;
use crate::queue::gpu::GpuUuid;

/// Longest reason kept with a snooze or an off switch
pub const MAX_REASON_LEN: usize = 1000;

/// Where an attempt's samples came from
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleSource {
    /// The local Netdata agent's `nvidia_smi` charts
    Netdata,
    /// `nvidia-smi --query-gpu` probes
    NvidiaSmi,
}

impl SampleSource {
    /// Stable name, also the storage tag
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Netdata => "netdata",
            Self::NvidiaSmi => "nvidia_smi",
        }
    }

    /// Parse a storage tag
    #[must_use]
    pub fn from_storage(raw: &str) -> Option<Self> {
        match raw {
            "netdata" => Some(Self::Netdata),
            "nvidia_smi" => Some(Self::NvidiaSmi),
            _ => None,
        }
    }
}

/// One idle alert, tied to the exact attempt and idle episode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuIdleNotice {
    /// Run task of the attempt
    pub attempt: TaskId,
    /// Idle episode of the attempt, starting at 1; busy work ends an episode
    pub episode: u32,
    /// 0 for the episode's first alert, then 1, 2, ... for reminders
    pub reminder: u32,
    /// The attempt never had a busy bucket
    pub never_busy: bool,
    /// GPU the attempt holds
    pub gpu_uuid: GpuUuid,
    /// Start of the episode's first idle bucket
    pub idle_since: DateTime<Utc>,
    /// Seconds from `idle_since` to the alert
    pub idle_secs: u64,
    /// Length of the window the counts cover, in seconds
    pub window_secs: u64,
    /// Idle buckets in the window
    pub idle_buckets: u32,
    /// Idle and busy buckets in the window; the rest had too few samples
    pub known_buckets: u32,
    /// Utilization percent below which a bucket is idle
    pub util_below: u8,
    /// Highest utilization in the window, in whole percent
    pub util_peak_pct: Option<u32>,
    /// Highest power draw in the window, in whole watts
    pub power_peak_watts: Option<u32>,
    /// `power_peak_watts` over the enforced power limit, in whole percent
    pub power_peak_pct: Option<u32>,
    /// Highest framebuffer memory in use in the window, in MiB
    pub memory_used_mib: Option<u64>,
    /// Framebuffer memory size, in MiB
    pub memory_total_mib: Option<u64>,
    /// Last write to the run's `output.log`
    pub last_output_at: Option<DateTime<Utc>>,
    /// The busiest process carrying the run's marker
    pub process: Option<ProcessSnapshot>,
    /// Where the samples came from
    pub source: SampleSource,
    /// What the owner should do now
    pub next_steps: String,
}

/// What `resource job gpu-idle` does
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum GpuIdleControl {
    /// No alerts until `duration` from when the queue records it
    Snooze {
        /// Any positive duration
        #[serde(with = "humantime_serde")]
        duration: Duration,
        /// Why, kept with the job
        reason: String,
    },
    /// No alerts for the rest of the job
    Off {
        /// Why, kept with the job
        reason: String,
    },
    /// Back to the job spec's setting
    Clear,
}

impl GpuIdleControl {
    /// Check the duration and reason before anything is stored
    ///
    /// # Errors
    ///
    /// A zero or unrepresentable duration, or an empty or overlong reason
    pub fn validate(&self) -> Result<(), String> {
        let reason = match self {
            Self::Snooze { duration, reason } => {
                if duration.is_zero() || chrono::Duration::from_std(*duration).is_err() {
                    return Err("snooze must be a positive duration that fits the clock".into());
                }
                reason
            }
            Self::Off { reason } => reason,
            Self::Clear => return Ok(()),
        };
        if reason.trim().is_empty() {
            return Err("give a --reason; it is kept with the job".into());
        }
        if reason.len() > MAX_REASON_LEN {
            return Err(format!("reason must be at most {MAX_REASON_LEN} bytes"));
        }
        Ok(())
    }

    /// Stable action name, also the storage tag
    #[must_use]
    pub const fn action(&self) -> &'static str {
        match self {
            Self::Snooze { .. } => "snooze",
            Self::Off { .. } => "off",
            Self::Clear => "clear",
        }
    }
}

/// One recorded control, as the queue stored it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuIdleControlRecord {
    /// `snooze`, `off`, or `clear`
    pub action: String,
    /// End of a snooze, computed once when the queue recorded it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<DateTime<Utc>>,
    /// Why the agent silenced the alerts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// When the queue recorded it
    pub at: DateTime<Utc>,
}

/// Whether a job's alerts are silenced right now
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Silence {
    /// Alerts are on
    Watching,
    /// A snooze runs until `until`
    Snoozed {
        /// End of the snooze
        until: DateTime<Utc>,
        /// Why
        reason: String,
    },
    /// An agent turned alerts off for the rest of the job
    Off {
        /// Why
        reason: String,
    },
    /// The job spec says `gpu_idle = "off"`
    SpecOff,
}

impl Silence {
    /// Whether alerts are held back
    #[must_use]
    pub fn silenced(&self) -> bool {
        !matches!(self, Self::Watching)
    }

    /// The silence the latest control leaves at `now`
    #[must_use]
    pub fn from_latest(
        latest: Option<&GpuIdleControlRecord>,
        spec_off: bool,
        now: DateTime<Utc>,
    ) -> Self {
        let spec = if spec_off {
            Self::SpecOff
        } else {
            Self::Watching
        };
        let Some(latest) = latest else {
            return spec;
        };
        let reason = latest.reason.clone().unwrap_or_default();
        match (latest.action.as_str(), latest.until) {
            ("snooze", Some(until)) if until > now => Self::Snoozed { until, reason },
            ("off", _) => Self::Off { reason },
            _ => spec,
        }
    }
}

/// The result of one recorded control, retained for operation replay
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuIdleControlResult {
    /// Job the control applies to
    pub job_id: crate::queue::JobId,
    /// The stored control
    pub control: GpuIdleControlRecord,
    /// The silence right after it
    pub silence: Silence,
}

/// One attempt's idle accounting, for `resource job show` and summaries
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuIdleAttemptView {
    /// Run task of the attempt
    pub task: TaskId,
    /// Current idle episode
    pub episode: u32,
    /// Alerts recorded
    pub alerts: u32,
    /// Idle buckets counted
    pub idle_buckets: u64,
    /// Busy buckets counted
    pub busy_buckets: u64,
    /// Buckets with too few samples
    pub unknown_buckets: u64,
    /// Start of the first busy bucket
    pub first_busy_at: Option<DateTime<Utc>>,
    /// Where the latest samples came from
    pub source: Option<SampleSource>,
}

/// A job's idle alert state, for `resource job show`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuIdleDetail {
    /// Whether alerts are silenced now
    pub silence: Silence,
    /// Every snooze, off switch, and clear, oldest first
    pub controls: Vec<GpuIdleControlRecord>,
    /// Idle accounting of each watched attempt
    pub attempts: Vec<GpuIdleAttemptView>,
}

/// The guidance every alert carries
#[must_use]
pub fn next_steps(job: crate::queue::JobId, task: TaskId) -> String {
    format!(
        "This job holds a GPU that has been near idle. A GPU job should hold its GPU only for GPU \
         work. Read `homebased task log {task} --tail 50` and the process state in this event. \
         If it is stuck or broken, cancel it: `homebased resource job cancel {job}`. If CPU work \
         (data prep, scoring, reports, uploads) runs inside the lease, cancel and resubmit it as a \
         chain with that work in its own task (`--after`). For a short expected gap such as a \
         large checkpoint write: `homebased resource job gpu-idle {job} --snooze 15m --reason \
         \"...\"`; to stop alerts for this job: `--off --reason \"...\"`. After 20m idle with no \
         snooze or off switch, any agent may cancel it."
    )
}
