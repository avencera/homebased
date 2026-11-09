//! `nvidia-smi --query-gpu` readings, the fallback when Netdata has no data
//!
//! Each probe is one bounded child process. A field the driver cannot report
//! reads `[N/A]` and is missing, never zero, so a failed or partial probe
//! never makes a GPU look idle

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::queue::gpu::GpuUuid;
use crate::queue::lease_guard::parse_compute_apps;

/// Fields of one reading, in query order
const READING_FIELDS: &str =
    "index,uuid,utilization.gpu,power.draw,enforced.power.limit,memory.used,memory.total";

const MIB: u64 = 1024 * 1024;

/// One GPU at one moment
#[derive(Debug, Clone, PartialEq)]
pub struct GpuReading {
    /// Device index in PCI bus order
    pub index: u32,
    /// GPU UUID
    pub uuid: GpuUuid,
    /// Utilization percent
    pub util: Option<f64>,
    /// Power draw in watts
    pub power_watts: Option<f64>,
    /// Enforced power limit in watts
    pub power_limit_watts: Option<f64>,
    /// Framebuffer memory in use, in bytes
    pub memory_used_bytes: Option<u64>,
    /// Framebuffer memory size, in bytes
    pub memory_total_bytes: Option<u64>,
}

fn number(field: &str) -> Option<f64> {
    let field = field.trim();
    if field.starts_with('[') {
        return None;
    }
    field.parse().ok().filter(|value: &f64| value.is_finite())
}

fn mebibytes(field: &str) -> Option<u64> {
    number(field)
        .filter(|value| *value >= 0.0)
        .map(|value| (value as u64).saturating_mul(MIB))
}

/// Readings from `--format=csv,noheader,nounits` output of [`READING_FIELDS`]
///
/// Lines without an index and a UUID are skipped
#[must_use]
pub fn parse_readings(output: &str) -> Vec<GpuReading> {
    output
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(',').map(str::trim).collect();
            let [index, uuid, util, power, limit, used, total] = fields.as_slice() else {
                return None;
            };
            Some(GpuReading {
                index: index.parse().ok()?,
                uuid: GpuUuid::parse(uuid).ok()?,
                util: number(util).filter(|value| (0.0..=100.0).contains(value)),
                power_watts: number(power).filter(|value| *value >= 0.0),
                power_limit_watts: number(limit).filter(|value| *value > 0.0),
                memory_used_bytes: mebibytes(used),
                memory_total_bytes: mebibytes(total),
            })
        })
        .collect()
}

/// Power limits from `--query-gpu=uuid,enforced.power.limit` output
#[must_use]
pub fn parse_power_limits(output: &str) -> HashMap<GpuUuid, f64> {
    output
        .lines()
        .filter_map(|line| {
            let (uuid, limit) = line.split_once(',')?;
            let uuid = GpuUuid::parse(uuid.trim()).ok()?;
            Some((uuid, number(limit).filter(|value| *value > 0.0)?))
        })
        .collect()
}

/// Run one CSV query, such as `--query-gpu=<fields>`
async fn run(query: &str, timeout: Duration) -> Result<String, String> {
    let mut command = tokio::process::Command::new("nvidia-smi");
    crate::run_env::scrub(command.as_std_mut())
        .arg(query)
        .arg("--format=csv,noheader,nounits");
    command.kill_on_drop(true);
    let output = tokio::time::timeout(timeout, command.output())
        .await
        .map_err(|_| format!("nvidia-smi did not answer within {timeout:?}"))?
        .map_err(|error| format!("nvidia-smi could not run: {error}"))?;
    if !output.status.success() {
        return Err(format!("nvidia-smi exited with {}", output.status));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Read every GPU once
///
/// # Errors
///
/// `nvidia-smi` is missing, fails, or exceeds `timeout`
pub async fn read(timeout: Duration) -> Result<Vec<GpuReading>, String> {
    run(&format!("--query-gpu={READING_FIELDS}"), timeout)
        .await
        .map(|output| parse_readings(&output))
}

/// Read each GPU's enforced power limit
///
/// # Errors
///
/// `nvidia-smi` is missing, fails, or exceeds `timeout`
pub async fn power_limits(timeout: Duration) -> Result<HashMap<GpuUuid, f64>, String> {
    run("--query-gpu=uuid,enforced.power.limit", timeout)
        .await
        .map(|output| parse_power_limits(&output))
}

/// PIDs holding a CUDA context, per GPU
///
/// # Errors
///
/// `nvidia-smi` is missing, fails, or exceeds `timeout`
pub async fn compute_apps(timeout: Duration) -> Result<HashMap<GpuUuid, HashSet<i32>>, String> {
    run("--query-compute-apps=gpu_uuid,pid", timeout)
        .await
        .map(|output| parse_compute_apps(&output))
}

#[cfg(test)]
mod tests {
    use super::{MIB, parse_power_limits, parse_readings};
    use crate::queue::gpu::GpuUuid;

    #[test]
    fn readings_treat_not_available_fields_as_missing() {
        let output = "\
0, GPU-6f3c1d2e-0000-1111-2222-333344445555, 0, 6.12, 575.00, 29000, 32607
1, GPU-7a4d2e3f-0000-1111-2222-333344445555, [N/A], [N/A], [N/A], [N/A], 24564
garbage line
";
        let readings = parse_readings(output);
        assert_eq!(readings.len(), 2);
        let first = &readings[0];
        assert_eq!(first.index, 0);
        assert_eq!(first.util, Some(0.0));
        assert_eq!(first.power_watts, Some(6.12));
        assert_eq!(first.power_limit_watts, Some(575.0));
        assert_eq!(first.memory_used_bytes, Some(29000 * MIB));
        let mig = &readings[1];
        assert_eq!(
            (mig.util, mig.power_watts, mig.memory_used_bytes),
            (None, None, None)
        );
        assert_eq!(mig.memory_total_bytes, Some(24564 * MIB));

        let limits =
            parse_power_limits("GPU-6f3c1d2e-0000-1111-2222-333344445555, 575.00\nGPU-x, [N/A]\n");
        assert_eq!(
            limits.get(&GpuUuid::parse("GPU-6f3c1d2e-0000-1111-2222-333344445555").unwrap()),
            Some(&575.0)
        );
        assert_eq!(limits.len(), 1);
    }
}
