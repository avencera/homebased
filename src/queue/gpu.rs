//! GPU detection for the machine's default resources
//!
//! On startup the daemon ensures one resource per detected GPU, named `gpu0`,
//! `gpu1`, and so on. Linux counts NVIDIA GPUs with `nvidia-smi -L`. A Mac has
//! one GPU, which becomes `gpu0` with no device index. A Linux machine that
//! has no `nvidia-smi` still gets an unindexed exclusive lane. An installed
//! probe that fails or returns no usable devices defers detection until a later start

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
    if platform == Platform::MacOs || nvidia_smi.is_none() {
        return vec![DetectedResource::unindexed()];
    }
    if indices.is_empty() {
        warn!("nvidia-smi returned no usable devices; GPU detection deferred");
        return Vec::new();
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
/// A missing `nvidia-smi` gives an unindexed lane. An installed but failing
/// probe defers resource detection until a later start
#[must_use]
pub fn detect() -> Vec<DetectedResource> {
    let platform = Platform::current();
    let output = match platform {
        Platform::MacOs => None,
        Platform::Linux => match run_nvidia_smi() {
            Ok(output) => output,
            Err(message) => {
                warn!("{message}; GPU detection deferred");
                return Vec::new();
            }
        },
    };
    let resources = resources_for(platform, output.as_deref());
    info!(count = resources.len(), "Detected GPU resources");
    resources
}

fn run_nvidia_smi() -> Result<Option<String>, String> {
    let mut command = Command::new("nvidia-smi");
    crate::run_env::scrub(&mut command).arg("-L");
    probe_nvidia_smi(&mut command)
}

fn probe_nvidia_smi(command: &mut Command) -> Result<Option<String>, String> {
    match command.output() {
        Ok(output) if output.status.success() => {
            Ok(Some(String::from_utf8_lossy(&output.stdout).into_owned()))
        }
        Ok(output) => Err(format!("nvidia-smi -L exited with {}", output.status)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            info!("nvidia-smi is not installed; using one unindexed resource");
            Ok(None)
        }
        Err(error) => Err(format!("nvidia-smi -L could not run: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DetectedResource, Platform, ResourceName, parse_nvidia_smi_list, probe_nvidia_smi,
        resources_for,
    };

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
        let resources = resources_for(Platform::Linux, None);
        assert_eq!(resources, vec![DetectedResource::unindexed()]);
        assert_eq!(resources[0].name.as_str(), "gpu0");
    }

    #[test]
    fn linux_unusable_probe_does_not_create_a_lane() {
        for output in ["", "No devices were found\n", "NVIDIA-SMI has failed\n"] {
            assert!(resources_for(Platform::Linux, Some(output)).is_empty());
        }
        assert_eq!(
            resources_for(Platform::Linux, None),
            vec![DetectedResource::unindexed()]
        );
    }

    #[test]
    fn probe_distinguishes_missing_from_installed_but_failing() {
        let dir = tempfile::tempdir().unwrap();
        let mut missing = std::process::Command::new(dir.path().join("missing"));
        assert_eq!(probe_nvidia_smi(&mut missing).unwrap(), None);
        let mut failing = std::process::Command::new("/bin/sh");
        failing.args(["-c", "exit 1"]);
        assert!(probe_nvidia_smi(&mut failing).is_err());
    }
}
