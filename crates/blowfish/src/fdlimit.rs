//! Raise the soft open-file limit to the hard limit at startup.
//!
//! A query opens segment and slab objects concurrently, so a namespace with a hundred or more
//! segments on `file://` storage can hold several hundred files at once. macOS starts processes
//! with a soft limit of 256, which failed those reads with EMFILE ("shared cache fetch failed",
//! then a 503 "segment is unavailable or corrupt"). Linux distributions usually start at 1,024.

/// Returns the soft limit before and after, or `None` when it could not be read or set.
#[cfg(unix)]
pub fn raise() -> Option<(u64, u64)> {
    // SAFETY: plain libc calls on a stack struct.
    unsafe {
        let mut r = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) != 0 {
            return None;
        }
        let before = r.rlim_cur;
        #[allow(unused_mut)]
        let mut want = r.rlim_max;
        // macOS rejects a soft limit above kern.maxfilesperproc (and RLIM_INFINITY).
        #[cfg(target_os = "macos")]
        {
            let mut max: libc::c_int = 0;
            let mut len = std::mem::size_of::<libc::c_int>();
            let ok = libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                (&mut max as *mut libc::c_int).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            ) == 0;
            want = want.min(if ok && max > 0 {
                max as libc::rlim_t
            } else {
                10_240
            });
        }
        if want <= before {
            return Some((before, before));
        }
        r.rlim_cur = want;
        if libc::setrlimit(libc::RLIMIT_NOFILE, &r) != 0 {
            return None;
        }
        Some((before, want))
    }
}

#[cfg(not(unix))]
pub fn raise() -> Option<(u64, u64)> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    #[test]
    fn raise_reaches_the_hard_limit_or_keeps_a_higher_soft_one() {
        let (before, after) = super::raise().expect("getrlimit and setrlimit");
        assert!(after >= before);
        // Idempotent: a second call finds nothing left to raise.
        assert_eq!(super::raise(), Some((after, after)));
    }
}
