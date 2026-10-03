//! Process table access on macOS through libproc and `sysctl(KERN_PROCARGS2)`

use std::ffi::c_int;
use std::io;
use std::mem;
use std::ptr;

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use super::{Environment, ProcessIdentity, ProcessInfo, ProcessStartTime, Read};

/// macOS has no pidfd, so a signal goes to the PID after the identity check
pub(crate) type SignalHandle = Pid;

/// Every PID in the process table
pub(super) fn list_pids() -> io::Result<Vec<Pid>> {
    // the table can grow between calls, so a full buffer is retried larger
    let mut capacity = 2048_usize;
    loop {
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes = c_int::try_from(capacity * mem::size_of::<libc::pid_t>())
            .map_err(|_| io::Error::other("process list buffer exceeds c_int"))?;
        // SAFETY: `pids` holds `capacity` initialised pid_t values and `bytes`
        // is its exact size, so the kernel writes only inside the buffer
        let count = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(count).map_err(io::Error::other)?;
        if count < capacity {
            pids.truncate(count);
            return Ok(pids
                .into_iter()
                .filter(|pid| *pid > 0)
                .map(Pid::from_raw)
                .collect());
        }
        capacity *= 2;
    }
}

/// Start time, group, and owner from `PROC_PIDTBSDINFO`; a zombie is `Exited`
pub(super) fn process_info(pid: Pid) -> Read<ProcessInfo> {
    let Ok(size) = c_int::try_from(mem::size_of::<libc::proc_bsdinfo>()) else {
        return Read::Refused(Errno::EOVERFLOW);
    };
    // SAFETY: proc_bsdinfo holds only integers and byte arrays, so all zeroes
    // is a valid value
    let mut info: libc::proc_bsdinfo = unsafe { mem::zeroed() };
    // SAFETY: the buffer is one proc_bsdinfo and `size` is its exact size
    let written = unsafe {
        libc::proc_pidinfo(
            pid.as_raw(),
            libc::PROC_PIDTBSDINFO,
            0,
            ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if written <= 0 {
        return match Errno::last() {
            Errno::ESRCH => exited_identity(pid).map_or(Read::Gone, Read::Exited),
            errno => Read::Refused(errno),
        };
    }
    if written != size || i64::from(info.pbi_pid) != i64::from(pid.as_raw()) {
        return Read::Refused(Errno::EIO);
    }

    let Ok(pgid) = i32::try_from(info.pbi_pgid) else {
        return Read::Refused(Errno::EIO);
    };
    let start = ProcessStartTime(
        info.pbi_start_tvsec
            .saturating_mul(1_000_000)
            .saturating_add(info.pbi_start_tvusec),
    );
    if info.pbi_status == libc::SZOMB {
        return Read::Exited(ProcessIdentity { pid, start });
    }
    Read::Found(ProcessInfo {
        start,
        pgid: Pid::from_raw(pgid),
        uid: info.pbi_uid,
    })
}

// libc has no kinfo_proc binding; this is the leading extern_proc layout
// from sys/proc.h, through p_pid. No trailing fields are interpreted
#[repr(C)]
struct ProcStatusPrefix {
    start: libc::timeval,
    _vmspace: *mut libc::c_void,
    _sigacts: *mut libc::c_void,
    _flags: c_int,
    status: u8,
    pid: libc::pid_t,
}

fn exited_identity(pid: Pid) -> Option<ProcessIdentity> {
    // proc_pidinfo excludes unreaped zombies, but KERN_PROC_PID reports their
    // status and start time, so they need not look newly vanished on every scan
    let mut mib = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_PID,
        pid.as_raw(),
    ];
    let mut buffer = [0_u8; 1024];
    let mut size = buffer.len();
    // safety: the kernel writes at most size bytes into the initialized buffer
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            4,
            buffer.as_mut_ptr().cast(),
            &mut size,
            ptr::null_mut(),
            0,
        )
    };
    if result != 0 || size < mem::size_of::<ProcStatusPrefix>() || size > buffer.len() {
        return None;
    }
    // safety: this initialized prefix consists only of integers and raw pointers;
    // read_unaligned does not require the byte buffer to have struct alignment
    let info = unsafe { buffer.as_ptr().cast::<ProcStatusPrefix>().read_unaligned() };
    if info.pid != pid.as_raw() || u32::from(info.status) != libc::SZOMB {
        return None;
    }
    let seconds = u64::try_from(info.start.tv_sec)
        .ok()
        .filter(|seconds| *seconds > 0)?;
    let micros = u64::try_from(info.start.tv_usec)
        .ok()
        .filter(|micros| *micros < 1_000_000)?;
    let start = seconds.checked_mul(1_000_000)?.checked_add(micros)?;
    Some(ProcessIdentity {
        pid,
        start: ProcessStartTime(start),
    })
}

/// Reads environments into one buffer of `KERN_ARGMAX` bytes, reused across reads
pub(crate) struct EnvironmentReader {
    buffer: Vec<u8>,
}

impl EnvironmentReader {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            buffer: vec![0; argmax()?],
        })
    }

    /// The environment block from `sysctl(KERN_PROCARGS2)`
    ///
    /// Apple platform binaries return an empty environment, and other users'
    /// processes are refused with `EINVAL`
    pub(super) fn read(&mut self, pid: Pid) -> Read<Environment> {
        let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid.as_raw()];
        let mut size = self.buffer.len();
        // SAFETY: `mib` holds 3 names; the kernel writes at most `size` bytes
        // into `buffer`, which is `size` bytes long, and stores the written
        // length back into `size`
        let status = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                3,
                self.buffer.as_mut_ptr().cast(),
                &mut size,
                ptr::null_mut(),
                0,
            )
        };
        if status != 0 {
            return match Errno::last() {
                Errno::ESRCH => Read::Gone,
                errno => Read::Refused(errno),
            };
        }
        match self.buffer.get(..size).and_then(environment_block) {
            Some(block) => Read::Found(Environment::from_block(block.to_vec())),
            None => Read::Refused(Errno::EBADMSG),
        }
    }
}

/// Largest argument and environment area, which bounds a `KERN_PROCARGS2` read
fn argmax() -> io::Result<usize> {
    let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
    let mut value: c_int = 0;
    let mut size = mem::size_of::<c_int>();
    // SAFETY: `mib` holds 2 names and the output is one c_int of `size` bytes
    let status = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            ptr::from_mut(&mut value).cast(),
            &mut size,
            ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    usize::try_from(value).map_err(io::Error::other)
}

/// The environment entries of a `KERN_PROCARGS2` buffer
///
/// The layout is `argc` as a native i32, the executable path, NUL padding,
/// `argc` NUL-terminated arguments, then NUL-terminated environment entries
/// up to an empty entry. An empty first argument is indistinguishable from the
/// padding, which can shift one argument into the environment; a process
/// would need an argument that is exactly a run marker entry for that to
/// matter
fn environment_block(buffer: &[u8]) -> Option<&[u8]> {
    let argc = i32::from_ne_bytes(buffer.get(..4)?.try_into().ok()?);
    let argc = usize::try_from(argc).ok()?;
    let mut rest = buffer.get(4..)?;

    let path_end = rest.iter().position(|byte| *byte == 0)?;
    rest = &rest[path_end..];
    let padding = rest.iter().take_while(|byte| **byte == 0).count();
    rest = &rest[padding..];
    for _ in 0..argc {
        let end = rest.iter().position(|byte| *byte == 0)?;
        rest = &rest[end + 1..];
    }

    let mut end = 0;
    while let Some(length) = rest[end..].iter().position(|byte| *byte == 0) {
        if length == 0 {
            break;
        }
        end += length + 1;
    }
    Some(&rest[..end])
}

pub(super) fn pin(pid: Pid) -> Read<SignalHandle> {
    Read::Found(pid)
}

pub(super) fn send(pid: &SignalHandle, signal: Signal) -> Result<(), Errno> {
    kill(*pid, signal)
}

#[cfg(test)]
mod tests {
    use super::environment_block;

    fn procargs(argc: i32, body: &[u8]) -> Vec<u8> {
        let mut buffer = argc.to_ne_bytes().to_vec();
        buffer.extend_from_slice(body);
        buffer
    }

    #[test]
    fn environment_follows_the_path_padding_and_arguments() {
        let buffer = procargs(
            2,
            b"/usr/bin/tool\0\0\0\0tool\0--flag\0A=1\0B=2\0\0\0garbage",
        );
        assert_eq!(environment_block(&buffer), Some(&b"A=1\0B=2\0"[..]));
    }

    #[test]
    fn missing_environment_is_empty_and_truncated_buffers_are_refused() {
        assert_eq!(
            environment_block(&procargs(1, b"/bin/sleep\0\0sleep\0\0\0")),
            Some(&b""[..])
        );
        assert_eq!(
            environment_block(&procargs(1, b"/bin/sleep\0\0sleep\0")),
            Some(&b""[..])
        );
        assert_eq!(environment_block(&procargs(3, b"/bin/x\0x\0y")), None);
        assert_eq!(environment_block(b"\x01\0"), None);
    }
}
