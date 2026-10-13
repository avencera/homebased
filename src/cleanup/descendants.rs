//! Descendants of a run's workload child, recorded while the run is alive
//!
//! A process that leaves the child's process group and drops the run marker is
//! tied to the run only by ancestry. Codex, for one, starts each MCP server in
//! its own process group with a scrubbed environment. Ancestry is lost once a
//! parent exits and its children are reparented, so the worker records the tree
//! while it is intact and hands the record to [`super::sweep_run`]
//!
//! A process running a homebased executable bounds the tree. A run may start
//! a daemon (`homebased daemon start` from a task), and that daemon, its
//! workers, and their workloads must outlive the run. Executables are compared
//! by file name, so a development build or a binary an update replaced still
//! bounds the tree, and a process whose executable cannot be read is treated as
//! a boundary

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use nix::unistd::Pid;
use tracing::warn;

use super::{ProcessIdentity, Read, os};

/// Running processes that descended from one workload child
#[derive(Debug)]
pub struct DescendantTracker {
    root: ProcessIdentity,
    /// Executable file name whose processes, and their descendants, are never
    /// recorded; `None` when it is unknown, which records nothing
    boundary: Option<OsString>,
    recorded: BTreeSet<ProcessIdentity>,
}

impl DescendantTracker {
    /// Track the descendants of `root`, the workload child, up to any process
    /// running an executable with this one's file name
    #[must_use]
    pub fn new(root: ProcessIdentity) -> Self {
        let own = std::env::current_exe();
        let boundary = own.as_deref().ok().and_then(executable_name);
        if boundary.is_none() {
            warn!(%root, ?own, "own executable name unknown; run descendants are not recorded");
        }
        Self::with_boundary(root, boundary)
    }

    pub(crate) fn with_boundary(root: ProcessIdentity, boundary: Option<OsString>) -> Self {
        Self {
            root,
            boundary,
            recorded: BTreeSet::new(),
        }
    }

    /// Descendants that were still running at the last observation
    #[must_use]
    pub fn recorded(&self) -> &BTreeSet<ProcessIdentity> {
        &self.recorded
    }

    /// The recorded descendants for [`super::sweep_run`], each checked against
    /// the boundary again, since a process recorded between fork and exec may
    /// have become a homebased process since
    #[must_use]
    pub fn into_adopted(self) -> BTreeSet<ProcessIdentity> {
        self.recorded
            .iter()
            .copied()
            .filter(|identity| !self.is_boundary(identity.pid))
            .collect()
    }

    /// Record the current descendants and forget recorded ones that exited
    ///
    /// A failed read keeps the previous record, and the next observation retries
    pub fn observe(&mut self) {
        if let Err(error) = self.try_observe() {
            warn!(root = %self.root, "record run descendants: {error}");
        }
    }

    fn try_observe(&mut self) -> io::Result<()> {
        let mut live = BTreeMap::new();
        for pid in os::list_pids()? {
            if let Read::Found(info) = os::process_info(pid) {
                live.insert(pid, info);
            }
        }
        let running = |identity: &ProcessIdentity| {
            live.get(&identity.pid)
                .is_some_and(|info| info.start == identity.start)
        };

        // a recorded process stays recorded after its parent exits and it is
        // reparented, which is exactly when only this record ties it to the run
        self.recorded.retain(running);

        let mut children: BTreeMap<Pid, Vec<ProcessIdentity>> = BTreeMap::new();
        for (pid, info) in &live {
            children
                .entry(info.ppid)
                .or_default()
                .push(ProcessIdentity {
                    pid: *pid,
                    start: info.start,
                });
        }

        // expanding recorded processes too catches what an orphaned descendant starts
        let mut parents: Vec<ProcessIdentity> = self.recorded.iter().copied().collect();
        if running(&self.root) {
            parents.push(self.root);
        }
        while let Some(parent) = parents.pop() {
            for child in children.remove(&parent.pid).unwrap_or_default() {
                // a child cannot start before its parent, so an older process
                // under a reused parent PID was read from a different tree
                if child.start < parent.start
                    || self.recorded.contains(&child)
                    || self.is_boundary(child.pid)
                {
                    continue;
                }
                self.recorded.insert(child);
                parents.push(child);
            }
        }
        Ok(())
    }

    fn is_boundary(&self, pid: Pid) -> bool {
        let Some(boundary) = &self.boundary else {
            return true;
        };
        os::executable_path(pid)
            .as_deref()
            .and_then(executable_name)
            .is_none_or(|name| name == *boundary)
    }
}

/// File name of an executable, without the ` (deleted)` that Linux appends
/// once an update replaces the file
fn executable_name(path: &Path) -> Option<OsString> {
    let name = path.file_name()?.as_bytes();
    let name = name.strip_suffix(b" (deleted)").unwrap_or(name);
    Some(OsStr::from_bytes(name).to_os_string())
}
