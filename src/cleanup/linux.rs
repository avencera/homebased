//! Process table access on Linux through `/proc` and pidfds

use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::ptr;

use nix::errno::Errno;
use nix::sys::signal::Signal;
use nix::unistd::Pid;

use super::{Environment, ProcessInfo, ProcessStartTime, Read};

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

/// Start time, group, and state from `/proc/<pid>/stat`, owner from `/proc/<pid>`
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
    match parse_stat(&stat) {
        Some((state, pgid, start)) => Read::Found(ProcessInfo {
            start: ProcessStartTime(start),
            pgid: Pid::from_raw(pgid),
            uid,
            zombie: matches!(state, 'Z' | 'X'),
        }),
        None => Read::Refused(Errno::EBADMSG),
    }
}

/// State (field 3), process group (field 5), and start time (field 22)
///
/// The command name in field 2 may hold spaces and parentheses, so fields are
/// counted from the last `)`
fn parse_stat(stat: &str) -> Option<(char, i32, u64)> {
    let after_name = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = after_name.split_whitespace().collect();
    let state = fields.first()?.chars().next()?;
    let pgid = fields.get(2)?.parse().ok()?;
    let start = fields.get(19)?.parse().ok()?;
    Some((state, pgid, start))
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
    use super::parse_stat;

    #[test]
    fn stat_fields_count_from_the_last_parenthesis() {
        let stat = "4242 (odd ) name) S 1 4240 4240 0 -1 4194560 120 0 0 0 1 2 0 0 20 0 1 0 987654 1000 200";
        assert_eq!(parse_stat(stat), Some(('S', 4240, 987_654)));
        assert_eq!(parse_stat("4242 (short) S 1"), None);
    }
}
