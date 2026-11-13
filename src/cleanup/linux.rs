//! Process table access on Linux through `/proc` and pidfds

use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::ptr;

use nix::errno::Errno;
use nix::sys::signal::Signal;
use nix::unistd::Pid;

use super::{Environment, ProcessIdentity, ProcessInfo, ProcessName, ProcessStartTime, Read};

/// A pidfd pins the process, so a signal sent through it cannot reach a later
/// process that reused the PID
pub(crate) type SignalHandle = OwnedFd;

/// Every PID under `/proc`
pub(super) fn list_pids() -> io::Result<Vec<Pid>> {
    let mut pids = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        if let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
            .filter(|pid| *pid > 0)
        {
            pids.push(Pid::from_raw(pid));
        }
    }
    Ok(pids)
}

/// Name, start time, parent, group, and state from `/proc/<pid>/stat`, owner from `/proc/<pid>`
pub(super) fn process_info(pid: Pid) -> Read<ProcessInfo> {
    let directory = format!("/proc/{pid}");
    let stat = match fs::read_to_string(format!("{directory}/stat")) {
        Ok(stat) => stat,
        Err(error) => return read_error(&error),
    };
    let uid = match fs::metadata(&directory) {
        Ok(metadata) => metadata.uid(),
        Err(error) => return read_error(&error),
    };
    let Some(stat) = parse_stat(&stat) else {
        return Read::Refused(Errno::EBADMSG);
    };
    // an unreaped zombie keeps its stat entry, so report its exit with the
    // identity it ran under, as the macOS reader does
    if matches!(stat.state, 'Z' | 'X') {
        return Read::Exited(ProcessIdentity {
            pid,
            start: ProcessStartTime(stat.start),
        });
    }
    Read::Found(ProcessInfo {
        start: ProcessStartTime(stat.start),
        ppid: Pid::from_raw(stat.ppid),
        pgid: Pid::from_raw(stat.pgid),
        uid,
        name: ProcessName::new(stat.name.as_bytes()),
    })
}

/// The executable a live process runs, or `None` when the read fails
///
/// The kernel appends ` (deleted)` once the file is replaced, as an update does,
/// so the suffix is dropped to compare against the path that now holds it
pub(super) fn executable_path(pid: Pid) -> Option<PathBuf> {
    let path = fs::read_link(format!("/proc/{pid}/exe")).ok()?;
    let bytes = path.as_os_str().as_bytes();
    Some(match bytes.strip_suffix(b" (deleted)") {
        Some(original) => Path::new(std::ffi::OsStr::from_bytes(original)).to_path_buf(),
        None => path,
    })
}

/// The fields of `/proc/<pid>/stat` that cleanup reads
#[derive(Debug, PartialEq, Eq)]
struct Stat<'a> {
    /// Command name (field 2)
    name: &'a str,
    /// State (field 3)
    state: char,
    /// Parent (field 4)
    ppid: i32,
    /// Process group (field 5)
    pgid: i32,
    /// Start time in clock ticks since boot (field 22)
    start: u64,
}

/// The command name in field 2 may hold spaces and parentheses, so it runs
/// from the first `(` to the last `)`, and later fields are counted from there
fn parse_stat(stat: &str) -> Option<Stat<'_>> {
    let name_start = stat.find('(')? + 1;
    let name_end = stat.rfind(')')?;
    let name = stat.get(name_start..name_end)?;
    let fields: Vec<&str> = stat[name_end + 1..].split_whitespace().collect();
    Some(Stat {
        name,
        state: fields.first()?.chars().next()?,
        ppid: fields.get(1)?.parse().ok()?,
        pgid: fields.get(2)?.parse().ok()?,
        start: fields.get(19)?.parse().ok()?,
    })
}

fn read_error<T>(error: &io::Error) -> Read<T> {
    match error.raw_os_error().map(Errno::from_raw) {
        Some(Errno::ENOENT | Errno::ESRCH) => Read::Gone,
        Some(errno) => Read::Refused(errno),
        None => Read::Refused(Errno::EIO),
    }
}

/// Reads `/proc/<pid>/environ`; kernel threads and zombies have an empty one
pub(crate) struct EnvironmentReader;

impl EnvironmentReader {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self)
    }

    pub(super) fn read(&mut self, pid: Pid) -> Read<Environment> {
        match fs::read(format!("/proc/{pid}/environ")) {
            Ok(block) => Read::Found(Environment::from_block(block)),
            Err(error) => read_error(&error),
        }
    }
}

pub(super) fn pin(pid: Pid) -> Read<SignalHandle> {
    // SAFETY: pidfd_open takes a PID and flags and returns a new descriptor or -1
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid.as_raw(), 0) };
    if fd < 0 {
        return match Errno::last() {
            Errno::ESRCH => Read::Gone,
            errno => Read::Refused(errno),
        };
    }
    let Ok(fd) = i32::try_from(fd) else {
        return Read::Refused(Errno::EBADF);
    };
    // SAFETY: the kernel just returned this descriptor, and nothing else owns it
    Read::Found(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub(super) fn send(pidfd: &SignalHandle, signal: Signal) -> Result<(), Errno> {
    // SAFETY: pidfd_send_signal takes an open pidfd, a signal number, no
    // siginfo, and no flags
    let status = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            signal as libc::c_int,
            ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if status < 0 {
        return Err(Errno::last());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Stat, parse_stat};

    #[test]
    fn stat_fields_count_from_the_last_parenthesis() {
        let stat = "4242 (odd ) name) S 1 4240 4240 0 -1 4194560 120 0 0 0 1 2 0 0 20 0 1 0 987654 1000 200";
        assert_eq!(
            parse_stat(stat),
            Some(Stat {
                name: "odd ) name",
                state: 'S',
                ppid: 1,
                pgid: 4240,
                start: 987_654,
            })
        );
        assert_eq!(parse_stat("4242 (short) S 1"), None);
    }
}
