//! GPU detection for the machine's default resources
//!
//! On startup the daemon ensures one resource per detected GPU, named `gpu0`,
//! `gpu1`, and so on. Linux counts NVIDIA GPUs with `nvidia-smi -L`. A Mac has
//! one GPU, which becomes `gpu0` with no device index. A Linux machine that
//! has no `nvidia-smi` still gets an unindexed exclusive lane. An installed
//! probe that fails or returns no usable devices defers detection until a later start
//!
//! The same output names each GPU's UUID, which stays with the physical card
//! when indices move, so later readers can tell which card a run held

use std::fmt;
use std::process::Command;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::ResourceName;

/// A GPU's UUID as `nvidia-smi` reports it, such as `GPU-6f3c1d2e-...`
///
/// Kept as the driver prints it: Netdata labels and `nvidia-smi --query-gpu`
/// use the same text, so it is compared as an opaque token
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GpuUuid(String);

impl GpuUuid {
    /// Longest UUID accepted; NVIDIA's are 40 characters
    const MAX_LEN: usize = 128;

    /// Accept one non-empty token of printable ASCII
    ///
    /// # Errors
    ///
    /// Refuses empty, overlong, and non-printable text, and text with spaces
    /// or parentheses, which cannot come from one `(UUID: ...)` field
    pub fn parse(raw: &str) -> Result<Self, String> {
        let valid = !raw.is_empty()
            && raw.len() <= Self::MAX_LEN
            && raw
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && byte != b'(' && byte != b')');
        if valid {
            Ok(Self(raw.to_owned()))
        } else {
            Err(format!("invalid GPU UUID {raw:?}"))
        }
    }

    /// The UUID text
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for GpuUuid {
    type Error = String;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<GpuUuid> for String {
    fn from(uuid: GpuUuid) -> Self {
        uuid.0
    }
}

impl fmt::Display for GpuUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One GPU line of `nvidia-smi -L`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGpu {
    /// Device index, the one `CUDA_VISIBLE_DEVICES` takes in PCI bus order
    pub index: u32,
    /// UUID, when the line names one
    pub uuid: Option<GpuUuid>,
}

/// A resource that detection says this machine has
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedResource {
    /// Default name, such as `gpu0`
    pub name: ResourceName,
    /// GPU index exported to runs, if the platform has one
    pub device: Option<u32>,
    /// UUID of the indexed GPU, when the probe named one
    pub uuid: Option<GpuUuid>,
}

impl DetectedResource {
    /// The lane every machine gets when no indexed GPU was found
    #[must_use]
    pub fn unindexed() -> Self {
        Self {
            name: ResourceName::gpu(0),
            device: None,
            uuid: None,
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

/// GPUs from `nvidia-smi -L` output, in ascending index order
///
/// Each GPU is a line such as `GPU 0: NVIDIA GeForce RTX 5090 (UUID: GPU-...)`.
/// Indented MIG device lines and anything else are ignored, and a repeated
/// index counts once, with its first line's UUID
#[must_use]
pub fn parse_nvidia_smi_list(output: &str) -> Vec<ListedGpu> {
    let mut gpus: Vec<ListedGpu> = output
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("GPU ")?;
            let (index, description) = rest.split_once(':')?;
            Some(ListedGpu {
                index: index.parse().ok()?,
                uuid: listed_uuid(description),
            })
        })
        .collect();
    // a stable sort keeps the first line of a repeated index ahead of the rest
    gpus.sort_by_key(|gpu| gpu.index);
    gpus.dedup_by_key(|gpu| gpu.index);
    gpus
}

/// The `(UUID: ...)` field of one GPU line, if it holds a usable UUID
fn listed_uuid(description: &str) -> Option<GpuUuid> {
    let (_, rest) = description.rsplit_once("(UUID: ")?;
    let (uuid, _) = rest.split_once(')')?;
    GpuUuid::parse(uuid.trim()).ok()
}

/// Resources for a platform, given the `nvidia-smi -L` output when it ran
#[must_use]
pub fn resources_for(platform: Platform, nvidia_smi: Option<&str>) -> Vec<DetectedResource> {
    let gpus = match platform {
        Platform::MacOs => Vec::new(),
        Platform::Linux => nvidia_smi.map(parse_nvidia_smi_list).unwrap_or_default(),
    };
    if platform == Platform::MacOs || nvidia_smi.is_none() {
        return vec![DetectedResource::unindexed()];
    }
    if gpus.is_empty() {
        warn!("nvidia-smi returned no usable devices; GPU detection deferred");
        return Vec::new();
    }
    gpus.into_iter()
        .map(|gpu| DetectedResource {
            name: ResourceName::gpu(gpu.index),
            device: Some(gpu.index),
            uuid: gpu.uuid,
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
        DetectedResource, GpuUuid, Platform, ResourceName, parse_nvidia_smi_list, probe_nvidia_smi,
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

    fn indexed(index: u32, uuid: &str) -> DetectedResource {
        DetectedResource {
            name: ResourceName::gpu(index),
            device: Some(index),
            uuid: Some(GpuUuid::parse(uuid).unwrap()),
        }
    }

    #[test]
    fn nvidia_smi_fixtures_parse_to_device_indices_and_uuids() {
        let cases = [
            (
                TWO_GPUS,
                vec![
                    (0, Some("GPU-6f3c1d2e-0000-1111-2222-333344445555")),
                    (1, Some("GPU-7a4d2e3f-0000-1111-2222-333344445555")),
                ],
            ),
            (
                MIG,
                vec![
                    (0, Some("GPU-11111111-2222-3333-4444-555555555555")),
                    (1, Some("GPU-22222222-2222-3333-4444-555555555555")),
                ],
            ),
            ("", vec![]),
            ("No devices were found\n", vec![]),
            (
                "NVIDIA-SMI has failed because it couldn't communicate\n",
                vec![],
            ),
            (
                "GPU 1: second (UUID: GPU-b)\nGPU 0: first\nGPU 1: again (UUID: GPU-c)\n",
                vec![(0, None), (1, Some("GPU-b"))],
            ),
            ("GPU 0: blank (UUID: )\n", vec![(0, None)]),
            ("GPU x: not a number\n", vec![]),
        ];
        for (output, expected) in cases {
            let parsed: Vec<_> = parse_nvidia_smi_list(output)
                .into_iter()
                .map(|gpu| (gpu.index, gpu.uuid.map(String::from)))
                .collect();
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(index, uuid)| (index, uuid.map(str::to_owned)))
                .collect();
            assert_eq!(parsed, expected, "{output:?}");
        }
    }

    #[test]
    fn linux_gets_one_resource_per_nvidia_gpu() {
        assert_eq!(
            resources_for(Platform::Linux, Some(TWO_GPUS)),
            vec![
                indexed(0, "GPU-6f3c1d2e-0000-1111-2222-333344445555"),
                indexed(1, "GPU-7a4d2e3f-0000-1111-2222-333344445555")
            ]
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
