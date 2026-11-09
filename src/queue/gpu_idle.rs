//! GPU idle alerts: a job holds its GPU only for GPU work
//!
//! While a run executes, the authority samples its GPU's utilization in fixed
//! buckets. A bucket whose peak stays below the threshold is idle, and an
//! attempt whose recent buckets are nearly all idle gets a `JOB_GPU_IDLE`
//! notice. Homebased only notifies: the job's owner decides whether to cancel,
//! split the CPU work out, or silence the alerts

pub mod netdata;
mod notice;
pub mod nvidia_smi;
pub mod process;
pub mod samples;
pub mod summary;
mod watch;

use std::fmt;
use std::time::Duration;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

pub use notice::{
    GpuIdleAttemptView, GpuIdleControl, GpuIdleControlRecord, GpuIdleControlResult, GpuIdleDetail,
    GpuIdleNotice, MAX_REASON_LEN, SampleSource, Silence, next_steps,
};
pub use summary::{GpuSummary, Metric, SummarySource, SummaryStatus};
pub use watch::{AlertDue, AttemptWatch, Bucket, BucketTotals, Episode, Thresholds, Verdict};

/// Alert setting of one job spec, the `gpu_idle` field
///
/// `"off"` turns alerts off for the whole job. The object form overrides the
/// machine's idle window, utilization threshold, or both
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuIdleSpec {
    /// No idle alerts for this job
    Off,
    /// Alerts with these overrides of the machine defaults
    Watch {
        /// How long the GPU must stay idle before the first alert
        after: Option<Duration>,
        /// Utilization percent below which a bucket counts as idle, 1 to 100
        util_below: Option<u8>,
    },
}

/// Why a `gpu_idle` value was refused, with the JSON pointer suffix it names
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuIdleSpecError {
    /// Pointer below `/gpu_idle`, empty for the field itself
    pub pointer: &'static str,
    /// What is wrong
    pub message: String,
}

impl fmt::Display for GpuIdleSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl GpuIdleSpec {
    /// Parse the `gpu_idle` value of a spec
    ///
    /// # Errors
    ///
    /// Refuses any string but `"off"`, unknown keys, an empty object, a
    /// duration that is zero or does not fit the clock, and `util_below`
    /// outside 1 to 100
    pub fn parse(value: &Value) -> Result<Self, GpuIdleSpecError> {
        let refuse = |pointer, message: &str| GpuIdleSpecError {
            pointer,
            message: message.to_owned(),
        };
        let object = match value {
            Value::String(text) if text == "off" => return Ok(Self::Off),
            Value::Object(object) => object,
            _ => {
                return Err(refuse(
                    "",
                    "gpu_idle is \"off\" or an object with after, util_below, or both",
                ));
            }
        };
        if let Some(key) = object
            .keys()
            .find(|key| !matches!(key.as_str(), "after" | "util_below"))
        {
            return Err(GpuIdleSpecError {
                pointer: "",
                message: format!("unknown gpu_idle field {key}; expected after or util_below"),
            });
        }
        if object.is_empty() {
            return Err(refuse(
                "",
                "give after, util_below, or both; use \"off\" to turn alerts off",
            ));
        }
        let after = object
            .get("after")
            .map(|after| {
                after
                    .as_str()
                    .and_then(|text| humantime::parse_duration(text).ok())
                    .filter(|after| !after.is_zero() && chrono::Duration::from_std(*after).is_ok())
                    .ok_or_else(|| {
                        refuse(
                            "/after",
                            "after must be a positive humantime duration that fits the clock, such as 10m",
                        )
                    })
            })
            .transpose()?;
        let util_below = object
            .get("util_below")
            .map(|util| {
                util.as_u64()
                    .filter(|util| (1..=100).contains(util))
                    .and_then(|util| u8::try_from(util).ok())
                    .ok_or_else(|| {
                        refuse("/util_below", "util_below must be an integer from 1 to 100")
                    })
            })
            .transpose()?;
        Ok(Self::Watch { after, util_below })
    }

    /// JSON Schema of the field, for `resource schema`
    #[must_use]
    pub fn schema() -> Value {
        json!({
            "description": "GPU idle alerts for this job. \"off\" turns them off; an object overrides the machine's idle window (after) or utilization threshold (util_below, percent). Alerts only notify; they never stop the job.",
            "oneOf": [
                { "const": "off" },
                {
                    "type": "object",
                    "properties": {
                        "after": {
                            "type": "string",
                            "description": "Humantime duration the GPU must stay idle before the first alert, such as 10m. Any positive duration."
                        },
                        "util_below": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 100,
                            "description": "Utilization percent below which the GPU counts as idle."
                        }
                    },
                    "minProperties": 1,
                    "additionalProperties": false
                }
            ]
        })
    }
}

impl Serialize for GpuIdleSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Off => serializer.serialize_str("off"),
            Self::Watch { after, util_below } => {
                let mut object = serde_json::Map::new();
                if let Some(after) = after {
                    object.insert(
                        "after".into(),
                        json!(humantime::format_duration(*after).to_string()),
                    );
                }
                if let Some(util) = util_below {
                    object.insert("util_below".into(), json!(util));
                }
                Value::Object(object).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for GpuIdleSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::parse(&value).map_err(D::Error::custom)
    }
}

/// Where the watcher reads GPU samples, from `resource.gpu_idle.source`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceChoice {
    /// Netdata when it has the GPU's charts, else `nvidia-smi`
    #[default]
    Auto,
    /// Netdata only
    Netdata,
    /// `nvidia-smi` probes only
    NvidiaSmi,
    /// No sampling and no alerts on this machine
    Off,
}

/// Machine thresholds a job spec's `gpu_idle` can override
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GpuIdleDefaults {
    /// Idle window before the first alert
    #[serde(with = "humantime_serde")]
    pub after: Duration,
    /// Utilization percent below which a bucket is idle
    pub util_below: u8,
    /// Bucket length and watcher tick
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Alert this long into an attempt that never got busy; twice `after`
    /// when unset or when the spec sets `after`
    #[serde(with = "humantime_serde")]
    pub never_busy_after: Option<Duration>,
    /// Continuous busy time that ends an idle episode
    #[serde(with = "humantime_serde")]
    pub recover_after: Duration,
    /// Gap before the first reminder; later ones come at 2x and 4x, then every 4x
    #[serde(with = "humantime_serde")]
    pub remind_after: Duration,
}

impl Default for GpuIdleDefaults {
    fn default() -> Self {
        Self {
            after: Duration::from_secs(5 * 60),
            util_below: 10,
            interval: Duration::from_secs(10),
            never_busy_after: None,
            recover_after: Duration::from_secs(60),
            remind_after: Duration::from_secs(15 * 60),
        }
    }
}

impl GpuIdleDefaults {
    /// Thresholds for a job with this spec setting
    ///
    /// A job with `gpu_idle = "off"` is still counted, so its summary is
    /// complete; the off switch only holds back alerts
    #[must_use]
    pub fn thresholds(&self, spec: Option<GpuIdleSpec>) -> Thresholds {
        let (after, util_below) = match spec {
            Some(GpuIdleSpec::Watch { after, util_below }) => (after, util_below),
            Some(GpuIdleSpec::Off) | None => (None, None),
        };
        let window = after.unwrap_or(self.after);
        let never_busy = match (after, self.never_busy_after) {
            (None, Some(configured)) => configured,
            _ => window.saturating_mul(2),
        };
        Thresholds {
            bucket: self.interval,
            window,
            never_busy,
            util_below: util_below.unwrap_or(self.util_below),
            recover_after: self.recover_after,
            remind_after: self.remind_after,
        }
    }
}

/// The `[resource.gpu_idle]` settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GpuIdleConfig {
    /// Where samples come from
    pub source: SourceChoice,
    /// Base URL of the local Netdata agent
    pub netdata_url: crate::fleet::address::MachineAddress,
    /// Thresholds a spec can override
    pub defaults: GpuIdleDefaults,
}

impl Default for GpuIdleConfig {
    fn default() -> Self {
        Self {
            source: SourceChoice::Auto,
            netdata_url: crate::fleet::address::MachineAddress::from_socket(
                std::net::SocketAddr::from(([127, 0, 0, 1], 19999)),
            ),
            defaults: GpuIdleDefaults::default(),
        }
    }
}
