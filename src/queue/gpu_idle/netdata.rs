//! Netdata local API client: GPU charts found by context and UUID, and range queries
//!
//! The go.d `nvidia_smi` collector names its charts after a user-chosen job
//! name, so a chart is never found by its id. It is found by its context and
//! matched to a GPU by its `uuid` label, or by the UUID inside its id when
//! the listing has no labels. Range queries are plain data: alerting and
//! summaries reduce the series themselves

use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use crate::fleet::address::MachineAddress;
use crate::fleet::http::ClusterClient;
use crate::queue::gpu::GpuUuid;

/// Context of the utilization chart, dimension `gpu`, in percent
pub const UTILIZATION_CONTEXT: &str = "nvidia_smi.gpu_utilization";
/// Context of the power chart, dimension `power_draw`, in watts
pub const POWER_CONTEXT: &str = "nvidia_smi.gpu_power_draw";
/// Context of the framebuffer chart, dimensions `free`, `used`, `reserved`, in bytes
pub const MEMORY_CONTEXT: &str = "nvidia_smi.gpu_frame_buffer_memory_usage";

/// Largest chart listing read; a busy node lists thousands of charts
const LISTING_LIMIT: usize = 32 * 1024 * 1024;
/// Largest range response read
const DATA_LIMIT: usize = 4 * 1024 * 1024;

/// One chart of a GPU
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chart {
    /// Chart id for data queries
    pub id: String,
    /// Seconds between collected samples
    pub update_every: u32,
}

/// The charts of one GPU; any of them may be missing
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GpuCharts {
    /// Utilization percent
    pub utilization: Option<Chart>,
    /// Power draw in watts
    pub power: Option<Chart>,
    /// Framebuffer memory in bytes
    pub memory: Option<Chart>,
}

/// How a range query reduces the samples inside each returned point
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    /// Highest sample
    Max,
    /// Mean of the samples
    Average,
}

impl Group {
    fn as_str(self) -> &'static str {
        match self {
            Self::Max => "max",
            Self::Average => "average",
        }
    }
}

/// One range query with absolute bounds in Unix seconds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeQuery<'a> {
    /// Chart id
    pub chart: &'a str,
    /// First second, inclusive
    pub after: i64,
    /// Last second, inclusive
    pub before: i64,
    /// Points to return over the range
    pub points: u32,
    /// Reduction inside each point
    pub group: Group,
}

impl RangeQuery<'_> {
    /// Request path, with `unaligned` so points follow the bounds exactly
    #[must_use]
    pub fn path(&self) -> String {
        format!(
            "/api/v1/data?chart={}&after={}&before={}&points={}&group={}&format=json&options=seconds,unaligned",
            encode(self.chart),
            self.after,
            self.before,
            self.points.max(1),
            self.group.as_str(),
        )
    }
}

/// Rows of one range response, oldest first, inside the query's bounds
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Series {
    /// Dimension names, without the leading `time`
    pub dimensions: Vec<String>,
    /// Unix second and one value per dimension; `None` where Netdata has none
    pub rows: Vec<(i64, Vec<Option<f64>>)>,
}

impl Series {
    /// Values of one dimension by time, skipping gaps
    pub fn values<'a>(&'a self, dimension: &str) -> impl Iterator<Item = (i64, f64)> + 'a {
        let column = self.dimensions.iter().position(|name| name == dimension);
        self.rows.iter().filter_map(move |(at, values)| {
            let value = values.get(column?).copied().flatten()?;
            Some((*at, value))
        })
    }

    /// Milliseconds between the returned rows: the smallest gap between two
    /// neighbours, or `fallback` with fewer than two rows
    ///
    /// Netdata returns fewer points than asked for when its collector or tier
    /// is coarser, so each row then stands for more time than requested. The
    /// smallest gap is used, so a hole in the data never widens the rows
    #[must_use]
    pub fn spacing_ms(&self, fallback: u64) -> u64 {
        self.rows
            .windows(2)
            .filter_map(|pair| u64::try_from(pair[1].0 - pair[0].0).ok())
            .filter(|gap| *gap > 0)
            .min()
            .map_or(fallback, |gap| gap.saturating_mul(1000))
    }

    /// Newest row time, if any row has a value
    #[must_use]
    pub fn newest(&self) -> Option<i64> {
        self.rows
            .iter()
            .filter(|(_, values)| values.iter().any(Option::is_some))
            .map(|(at, _)| *at)
            .max()
    }
}

/// The charts of each wanted GPU in a `/api/v1/charts` listing
///
/// # Errors
///
/// The body is not a chart listing
pub fn parse_charts(
    body: &[u8],
    wanted: &[GpuUuid],
) -> Result<HashMap<GpuUuid, GpuCharts>, String> {
    #[derive(Deserialize)]
    struct Listing {
        charts: HashMap<String, ListedChart>,
    }
    #[derive(Deserialize)]
    struct ListedChart {
        id: Option<String>,
        context: Option<String>,
        update_every: Option<u32>,
        chart_labels: Option<HashMap<String, Value>>,
        labels: Option<HashMap<String, Value>>,
    }

    let listing: Listing =
        serde_json::from_slice(body).map_err(|error| format!("chart listing: {error}"))?;
    let mut found: HashMap<GpuUuid, GpuCharts> = HashMap::new();
    // sorted so a duplicate chart resolves the same way on every listing
    let mut charts: Vec<_> = listing.charts.into_iter().collect();
    charts.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, chart) in charts {
        let Some(context) = chart.context.as_deref() else {
            continue;
        };
        let id = chart.id.unwrap_or(key);
        let label = chart
            .chart_labels
            .as_ref()
            .or(chart.labels.as_ref())
            .and_then(|labels| labels.get("uuid"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let lowered = id.to_ascii_lowercase();
        let Some(uuid) = wanted.iter().find(|uuid| match &label {
            Some(label) => label.eq_ignore_ascii_case(uuid.as_str()),
            None => lowered.contains(&uuid.as_str().to_ascii_lowercase()),
        }) else {
            continue;
        };
        let entry = found.entry(uuid.clone()).or_default();
        let slot = match context {
            UTILIZATION_CONTEXT => &mut entry.utilization,
            POWER_CONTEXT => &mut entry.power,
            MEMORY_CONTEXT => &mut entry.memory,
            _ => continue,
        };
        if slot.is_none() {
            *slot = Some(Chart {
                id,
                update_every: chart.update_every.unwrap_or(1).max(1),
            });
        }
    }
    Ok(found)
}

/// Rows of a `format=json&options=seconds` response
///
/// Rows outside `after..=before`, repeated times, and non-numeric values are
/// dropped, so a misbehaving server cannot place samples in another bucket
///
/// # Errors
///
/// The body is not a data response
pub fn parse_series(body: &[u8], after: i64, before: i64) -> Result<Series, String> {
    #[derive(Deserialize)]
    struct Data {
        labels: Vec<String>,
        data: Vec<Vec<Value>>,
    }

    let data: Data =
        serde_json::from_slice(body).map_err(|error| format!("data response: {error}"))?;
    let Some((time, dimensions)) = data.labels.split_first() else {
        return Err("data response has no labels".into());
    };
    if time != "time" {
        return Err(format!("data response starts with {time:?}, not time"));
    }
    let mut rows: Vec<(i64, Vec<Option<f64>>)> = data
        .data
        .into_iter()
        .filter_map(|row| {
            let (at, values) = row.split_first()?;
            let at = at
                .as_i64()
                .or_else(|| at.as_f64().map(|value| value as i64))?;
            if !(after..=before).contains(&at) {
                return None;
            }
            let values = (0..dimensions.len())
                .map(|index| values.get(index).and_then(Value::as_f64))
                .collect();
            Some((at, values))
        })
        .collect();
    rows.sort_by_key(|(at, _)| *at);
    rows.dedup_by_key(|(at, _)| *at);
    Ok(Series {
        dimensions: dimensions.to_vec(),
        rows,
    })
}

/// Percent-encode a chart id for a query string
fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// A Netdata agent's local API
#[derive(Debug, Clone)]
pub struct Netdata {
    address: MachineAddress,
    timeout: Duration,
}

impl Netdata {
    /// Client for the agent at `address`, with a bound on each request
    #[must_use]
    pub fn new(address: MachineAddress, timeout: Duration) -> Self {
        Self { address, timeout }
    }

    async fn get(&self, path: &str, limit: usize) -> Result<Vec<u8>, String> {
        let response = ClusterClient::new(self.timeout, limit)
            .get(&self.address, path)
            .await
            .map_err(|error| error.to_string())?;
        if !response.status.is_success() {
            return Err(format!("netdata answered {} for {path}", response.status));
        }
        Ok(response.body.to_vec())
    }

    /// The charts of each wanted GPU
    ///
    /// # Errors
    ///
    /// The agent is unreachable or its listing does not parse
    pub async fn charts(&self, wanted: &[GpuUuid]) -> Result<HashMap<GpuUuid, GpuCharts>, String> {
        let body = self.get("/api/v1/charts", LISTING_LIMIT).await?;
        parse_charts(&body, wanted)
    }

    /// One range of one chart
    ///
    /// # Errors
    ///
    /// The agent is unreachable or its response does not parse
    pub async fn range(&self, query: &RangeQuery<'_>) -> Result<Series, String> {
        let body = self.get(&query.path(), DATA_LIMIT).await?;
        parse_series(&body, query.after, query.before)
    }
}

#[cfg(test)]
mod tests {
    use super::{Group, RangeQuery, parse_charts, parse_series};
    use crate::queue::gpu::GpuUuid;

    const UUID: &str = "GPU-6f3c1d2e-0000-1111-2222-333344445555";
    const OTHER: &str = "GPU-7a4d2e3f-0000-1111-2222-333344445555";

    #[test]
    fn charts_match_by_label_or_by_the_uuid_in_the_id() {
        let listing = serde_json::json!({
            "hostname": "ai5090",
            "charts": {
                "nvidia_smi_rtx5090.gpu_gpu-6f3c1d2e-0000-1111-2222-333344445555_gpu_utilization": {
                    "id": "nvidia_smi_rtx5090.gpu_gpu-6f3c1d2e-0000-1111-2222-333344445555_gpu_utilization",
                    "context": "nvidia_smi.gpu_utilization",
                    "update_every": 1,
                    "chart_labels": { "uuid": UUID, "product_name": "NVIDIA GeForce RTX 5090" }
                },
                "custom.power_of_first": {
                    "id": "custom.power_of_first",
                    "context": "nvidia_smi.gpu_power_draw",
                    "update_every": 2,
                    "chart_labels": { "uuid": UUID }
                },
                // no labels: matched by the lowercase UUID in its id
                "nvidia_smi_rtx5090.gpu_gpu-6f3c1d2e-0000-1111-2222-333344445555_frame_buffer_memory_usage": {
                    "context": "nvidia_smi.gpu_frame_buffer_memory_usage"
                },
                "nvidia_smi_rtx4090.gpu_gpu-7a4d2e3f-0000-1111-2222-333344445555_gpu_utilization": {
                    "id": "nvidia_smi_rtx4090.gpu_gpu-7a4d2e3f-0000-1111-2222-333344445555_gpu_utilization",
                    "context": "nvidia_smi.gpu_utilization",
                    "chart_labels": { "uuid": OTHER }
                },
                "system.cpu": { "id": "system.cpu", "context": "system.cpu" }
            }
        });
        let wanted = [GpuUuid::parse(UUID).unwrap()];
        let found = parse_charts(listing.to_string().as_bytes(), &wanted).unwrap();
        assert_eq!(found.len(), 1, "only the wanted GPU");
        let charts = &found[&wanted[0]];
        assert!(
            charts
                .utilization
                .as_ref()
                .unwrap()
                .id
                .ends_with("_gpu_utilization")
        );
        let power = charts.power.as_ref().unwrap();
        assert_eq!(
            (power.id.as_str(), power.update_every),
            ("custom.power_of_first", 2)
        );
        assert!(charts.memory.as_ref().unwrap().id.contains("frame_buffer"));
        assert!(parse_charts(b"{}", &wanted).is_err());
    }

    #[test]
    fn series_keep_rows_inside_the_bounds_oldest_first() {
        // Netdata answers newest first; nulls are gaps
        let body = serde_json::json!({
            "labels": ["time", "free", "used", "reserved"],
            "data": [
                [1_700_000_012, 1.0, 9.0, 0.5],
                [1_700_000_011, null, 8.0, 0.5],
                [1_700_000_010, 1.0, 7.0, 0.5],
                [1_700_000_010, 1.0, 7.0, 0.5],
                [1_699_999_000, 1.0, 99.0, 0.5]
            ]
        });
        let series =
            parse_series(body.to_string().as_bytes(), 1_700_000_010, 1_700_000_012).unwrap();
        assert_eq!(series.dimensions, ["free", "used", "reserved"]);
        let used: Vec<_> = series.values("used").collect();
        assert_eq!(
            used,
            [
                (1_700_000_010, 7.0),
                (1_700_000_011, 8.0),
                (1_700_000_012, 9.0)
            ]
        );
        assert_eq!(series.values("free").count(), 2);
        assert_eq!(series.newest(), Some(1_700_000_012));
        assert!(parse_series(br#"{"labels":["gpu"],"data":[]}"#, 0, 1).is_err());
    }

    #[test]
    fn range_paths_use_absolute_unaligned_bounds() {
        let query = RangeQuery {
            chart: "nvidia_smi_a.gpu_gpu-x_gpu_utilization",
            after: 10,
            before: 19,
            points: 10,
            group: Group::Max,
        };
        assert_eq!(
            query.path(),
            "/api/v1/data?chart=nvidia_smi_a.gpu_gpu-x_gpu_utilization&after=10&before=19&points=10&group=max&format=json&options=seconds,unaligned"
        );
    }
}
