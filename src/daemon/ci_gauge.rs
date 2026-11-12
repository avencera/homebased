//! Load, cores, and free disk that CI admission reads

use std::path::{Path, PathBuf};

use crate::home::Home;
use crate::queue::ci::{CiConfig, Gauge};

/// Where slot clones live: `ci.checkout_root`, or `ci/checkouts` under the
/// daemon home
#[must_use]
pub(crate) fn checkout_root(home: &Home, config: &CiConfig) -> PathBuf {
    config
        .checkout_root
        .clone()
        .unwrap_or_else(|| home.root().join("ci").join("checkouts"))
}

/// Read the machine's 1-minute load, its cores, and the free space under
/// `root`
///
/// Each read is a single syscall. A load or disk that cannot be read leaves
/// admission to the other checks rather than closing the machine
#[must_use]
pub(crate) fn read_gauge(root: &Path) -> Gauge {
    let cores = std::thread::available_parallelism()
        .ok()
        .and_then(|cores| u32::try_from(cores.get()).ok())
        .unwrap_or(1);
    Gauge {
        load1: load1().unwrap_or(0.0),
        cores,
        free_disk: free_disk(root),
    }
}

fn load1() -> Option<f64> {
    let mut loads = [0.0_f64; 3];
    // SAFETY: `loads` has room for the 3 samples requested
    let read = unsafe { libc::getloadavg(loads.as_mut_ptr(), 3) };
    (read >= 1).then_some(loads[0])
}

/// Free bytes for unprivileged users on the filesystem holding `root`, or
/// its nearest existing ancestor before the first clone creates it
fn free_disk(root: &Path) -> Option<u64> {
    let existing = root.ancestors().find(|path| path.exists())?;
    let stats = nix::sys::statvfs::statvfs(existing).ok()?;
    // the field widths differ by platform
    #[allow(clippy::useless_conversion)]
    let free = u64::from(stats.blocks_available()).checked_mul(u64::from(stats.fragment_size()));
    free
}
