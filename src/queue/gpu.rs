//! GPU detection for the machine's default resources
//!
//! On startup the daemon ensures one resource per detected GPU, named `gpu0`,
//! `gpu1`, and so on. Linux counts NVIDIA GPUs with `nvidia-smi -L`. A Mac has
//! one GPU, which becomes `gpu0` with no device index. A machine where
//! detection finds nothing still gets `gpu0` with no device index, so it has
//! an exclusive lane

use std::process::Command;

use tracing::{info, warn};

use super::ResourceName;

/// A resource that detection says this machine has
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedResource {
    /// Default name, such as `gpu0`
    pub name: ResourceName,
    /// GPU index exported to runs, if the platform has one
    pub device: Option<u32>,
}

impl DetectedResource {
    /// The lane every machine gets when no indexed GPU was found
    #[must_use]
    pub fn unindexed() -> Self {
        Self {
            name: ResourceName::gpu(0),
            device: None,
        }
    }
}

/// Platform rules for detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// One GPU with no device index
    MacOs,
    /// NVIDIA GPUs from `nvidia-smi -L`
    Linux,
}

impl Platform {
    /// The platform this binary runs on
    #[must_use]
    pub const fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::MacOs
        } else {
            Self::Linux
        }
    }
}

/// Device indices from `nvidia-smi -L` output, in ascending order
///
/// Each GPU is a line such as `GPU 0: NVIDIA GeForce RTX 5090 (UUID: ...)`.
/// Indented MIG device lines and anything else are ignored, and a repeated
/// index counts once
#[must_use]
pub fn parse_nvidia_smi_list(output: &str) -> Vec<u32> {
    let mut indices: Vec<u32> = output
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("GPU ")?;
            let (index, _) = rest.split_once(':')?;
            index.parse().ok()
        })
        .collect();
    indices.sort_unstable();
    indices.dedup();
    indices
}

/// Resources for a platform, given the `nvidia-smi -L` output when it ran
#[must_use]
pub fn resources_for(platform: Platform, nvidia_smi: Option<&str>) -> Vec<DetectedResource> {
    let indices = match platform {
        Platform::MacOs => Vec::new(),
        Platform::Linux => nvidia_smi.map(parse_nvidia_smi_list).unwrap_or_default(),
    };
    if indices.is_empty() {
        return vec![DetectedResource::unindexed()];
    }
    indices
        .into_iter()
        .map(|index| DetectedResource {
            name: ResourceName::gpu(index),
            device: Some(index),
        })
        .collect()
}

/// Detect this machine's resources
///
/// A missing or failing `nvidia-smi` is not an error: the machine still gets
/// its unindexed `gpu0`
#[must_use]
pub fn detect() -> Vec<DetectedResource> {
    let platform = Platform::current();
    let output = match platform {
        Platform::MacOs => None,
        Platform::Linux => run_nvidia_smi(),
    };
    let resources = resources_for(platform, output.as_deref());
    info!(count = resources.len(), "Detected GPU resources");
    resources
}

fn run_nvidia_smi() -> Option<String> {
    let mut command = Command::new("nvidia-smi");
    crate::run_env::scrub(&mut command).arg("-L");
    match command.output() {
        Ok(output) if output.status.success() => {
            Some(String::from_utf8_lossy(&output.stdout).into_owned())
        }
        Ok(output) => {
            let status = output.status;
            warn!("nvidia-smi -L exited with {status}; using one unindexed resource");
            None
        }
        Err(error) => {
            info!("nvidia-smi unavailable ({error}); using one unindexed resource");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DetectedResource, Platform, ResourceName, parse_nvidia_smi_list, resources_for};

    const TWO_GPUS: &str = "\
GPU 0: NVIDIA GeForce RTX 5090 (UUID: GPU-6f3c1d2e-0000-1111-2222-333344445555)
GPU 1: NVIDIA GeForce RTX 4090 (UUID: GPU-7a4d2e3f-0000-1111-2222-333344445555)
";

    const MIG: &str = "\
GPU 0: NVIDIA A100-SXM4-40GB (UUID: GPU-11111111-2222-3333-4444-555555555555)
  MIG 1g.5gb      Device  0: (UUID: MIG-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee)
  MIG 1g.5gb      Device  1: (UUID: MIG-ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee)
GPU 1: NVIDIA A100-SXM4-40GB (UUID: GPU-22222222-2222-3333-4444-555555555555)
";

    fn indexed(index: u32) -> DetectedResource {
        DetectedResource {
            name: ResourceName::gpu(index),
            device: Some(index),
        }
    }

    #[test]
    fn nvidia_smi_fixtures_parse_to_device_indices() {
        let cases = [
            (TWO_GPUS, vec![0, 1]),
            (MIG, vec![0, 1]),
            ("", vec![]),
            ("No devices were found\n", vec![]),
            (
                "NVIDIA-SMI has failed because it couldn't communicate\n",
                vec![],
            ),
            ("GPU 1: second\nGPU 0: first\nGPU 1: again\n", vec![0, 1]),
            ("GPU x: not a number\n", vec![]),
        ];
        for (output, expected) in cases {
            assert_eq!(parse_nvidia_smi_list(output), expected, "{output:?}");
        }
    }

    #[test]
    fn linux_gets_one_resource_per_nvidia_gpu() {
        assert_eq!(
            resources_for(Platform::Linux, Some(TWO_GPUS)),
            vec![indexed(0), indexed(1)]
        );
    }

    #[test]
    fn macos_gets_one_unindexed_gpu0() {
        // even NVIDIA-looking output is ignored on macOS
        for output in [None, Some(TWO_GPUS)] {
            assert_eq!(
                resources_for(Platform::MacOs, output),
                vec![DetectedResource::unindexed()]
            );
        }
    }

    #[test]
    fn no_gpu_found_still_gives_an_unindexed_gpu0() {
        for output in [None, Some(""), Some("No devices were found\n")] {
            let resources = resources_for(Platform::Linux, output);
            assert_eq!(resources, vec![DetectedResource::unindexed()], "{output:?}");
            assert_eq!(resources[0].name.as_str(), "gpu0");
        }
    }
}
