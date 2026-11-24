// Shared Linux PMU counters for the resident and compiled-kernel diagnostics.
#[cfg(target_os = "linux")]
use std::{fs::File, io::Read};

#[cfg(target_os = "linux")]
pub(crate) struct Counter(File);
#[cfg(target_os = "linux")]
impl Counter {
    pub(crate) fn open(event: u64) -> std::io::Result<Self> {
        Self::with_type(0, event)
    }
    pub(crate) fn raw(event: u64) -> std::io::Result<Self> {
        Self::with_type(4, event)
    }
    pub(crate) fn with_type(kind: u64, event: u64) -> std::io::Result<Self> {
        use std::os::fd::FromRawFd;
        // perf_event_attr v0 (64 bytes): hardware event, user space only, disabled.
        let mut attr = [0u64; 8];
        attr[0] = (64u64 << 32) | kind;
        attr[1] = event;
        attr[4] = 3; // PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING.
        attr[5] = 1 | (1 << 5) | (1 << 6);
        // SAFETY: valid attr pointer, this thread, no CPU affinity, and no group.
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: perf_event_open returned a new owned file descriptor.
        Ok(Self(unsafe { File::from_raw_fd(fd as i32) }))
    }
    pub(crate) fn start(&self) {
        use std::os::fd::AsRawFd;
        // SAFETY: these perf ioctls take no pointer argument.
        unsafe {
            libc::ioctl(self.0.as_raw_fd(), 0x2403, 0);
            libc::ioctl(self.0.as_raw_fd(), 0x2400, 0);
        }
    }
    pub(crate) fn stop(mut self) -> (u64, u64, u64) {
        use std::os::fd::AsRawFd;
        // SAFETY: PERF_EVENT_IOC_DISABLE takes no pointer argument.
        unsafe {
            libc::ioctl(self.0.as_raw_fd(), 0x2401, 0);
        }
        let mut bytes = [0; 24];
        self.0.read_exact(&mut bytes).unwrap();
        let words: [u64; 3] = std::array::from_fn(|i| {
            u64::from_ne_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap())
        });
        (words[0], words[1], words[2])
    }
}
#[cfg(not(target_os = "linux"))]
pub(crate) struct Counter;
#[cfg(not(target_os = "linux"))]
impl Counter {
    pub(crate) fn raw(_event: u64) -> std::io::Result<Self> {
        Err(std::io::Error::other("hardware counts require Linux perf"))
    }
    pub(crate) fn open(_event: u64) -> std::io::Result<Self> {
        Err(std::io::Error::other("hardware counts require Linux perf"))
    }
    pub(crate) fn start(&self) {}
    pub(crate) fn stop(self) -> (u64, u64, u64) {
        (0, 0, 0)
    }
}
