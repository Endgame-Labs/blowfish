//! A last-words report for fatal signals (SIGSEGV, SIGBUS, SIGILL, and SIGFPE).
//!
//! On a crash, the handler writes one line to stderr naming the signal, the fault address, and
//! the thread, followed by the raw stack frames. It then restores the previous handler and
//! returns, so the faulting instruction runs again under the previous handler. Rust's own
//! handler still reports stack overflows, and otherwise the process dies with the same signal and
//! exit status as before.
//!
//! The handler avoids allocating. `backtrace_symbols_fd` writes straight to the file descriptor,
//! and frames inside blowfish print as offsets that `addr2line -e blowfish` resolves (release
//! builds carry debug info).

#[cfg(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos"))]
mod imp {
    use std::sync::atomic::{AtomicBool, Ordering};

    const SIGNALS: [libc::c_int; 4] = [libc::SIGSEGV, libc::SIGBUS, libc::SIGILL, libc::SIGFPE];
    static mut PREVIOUS: [std::mem::MaybeUninit<libc::sigaction>; 4] =
        [const { std::mem::MaybeUninit::uninit() }; 4];
    static INSTALLED: AtomicBool = AtomicBool::new(false);
    static REPORTED: AtomicBool = AtomicBool::new(false);

    /// A fixed-size line buffer, so the handler never allocates.
    struct Line {
        buf: [u8; 256],
        len: usize,
    }

    impl Line {
        fn push(&mut self, bytes: &[u8]) {
            let n = bytes.len().min(self.buf.len() - self.len);
            self.buf[self.len..self.len + n].copy_from_slice(&bytes[..n]);
            self.len += n;
        }
        fn hex(&mut self, mut v: usize) {
            let mut digits = [0u8; 16];
            let mut i = digits.len();
            loop {
                i -= 1;
                digits[i] = b"0123456789abcdef"[v & 15];
                v >>= 4;
                if v == 0 {
                    break;
                }
            }
            self.push(b"0x");
            self.push(&digits[i..]);
        }
    }

    fn name(signal: libc::c_int) -> &'static [u8] {
        match signal {
            libc::SIGSEGV => b"SIGSEGV",
            libc::SIGBUS => b"SIGBUS",
            libc::SIGILL => b"SIGILL",
            _ => b"SIGFPE",
        }
    }

    #[cfg(target_os = "linux")]
    unsafe fn fault_address(info: *const libc::siginfo_t) -> usize {
        unsafe { (*info).si_addr() as usize }
    }

    /// Whether another process sent the signal with kill(), rather than this one faulting. Linux
    /// marks these with codes of zero or below. macOS reports them like a real fault (code 2,
    /// address 0), so there they take the same path as a fault, as they did before this handler.
    unsafe fn sent_by_process(info: *const libc::siginfo_t) -> bool {
        cfg!(target_os = "linux") && unsafe { (*info).si_code } <= 0
    }

    #[cfg(target_os = "macos")]
    unsafe fn fault_address(info: *const libc::siginfo_t) -> usize {
        unsafe { (*info).si_addr as usize }
    }

    extern "C" fn handler(signal: libc::c_int, info: *mut libc::siginfo_t, _: *mut libc::c_void) {
        let index = SIGNALS.iter().position(|s| *s == signal).unwrap_or(0);
        if !REPORTED.swap(true, Ordering::SeqCst) {
            let mut line = Line {
                buf: [0; 256],
                len: 0,
            };
            line.push(b"\nblowfish ");
            line.push(env!("CARGO_PKG_VERSION").as_bytes());
            line.push(b": fatal ");
            line.push(name(signal));
            if unsafe { sent_by_process(info) } {
                line.push(b" (sent by another process)");
            } else {
                line.push(b" at fault address ");
                line.hex(unsafe { fault_address(info) });
            }
            line.push(b" on thread '");
            let mut thread = [0 as libc::c_char; 64];
            if unsafe {
                libc::pthread_getname_np(libc::pthread_self(), thread.as_mut_ptr(), thread.len())
            } == 0
            {
                let bytes = unsafe { std::ffi::CStr::from_ptr(thread.as_ptr()) }.to_bytes();
                line.push(bytes);
            }
            line.push(
                b"'; stack frames follow (resolve blowfish offsets with addr2line -e blowfish)\n",
            );
            unsafe { libc::write(2, line.buf.as_ptr().cast(), line.len) };
            let mut frames = [std::ptr::null_mut::<libc::c_void>(); 64];
            let n = unsafe { libc::backtrace(frames.as_mut_ptr(), frames.len() as libc::c_int) };
            unsafe { libc::backtrace_symbols_fd(frames.as_ptr(), n, 2) };
        }
        // Hand the signal back to whoever had it before (Rust's stack-overflow reporter, or the
        // default action) and let the instruction fault again.
        unsafe {
            let previous = &raw const PREVIOUS;
            libc::sigaction(signal, (*previous)[index].as_ptr(), std::ptr::null_mut());
            // A signal sent with kill() will not recur on its own, so take the default action
            // (terminate with this signal) once this handler returns.
            if sent_by_process(info) {
                libc::signal(signal, libc::SIG_DFL);
                libc::raise(signal);
            }
        }
    }

    pub fn install() {
        if INSTALLED.swap(true, Ordering::SeqCst) {
            return;
        }
        // glibc's backtrace loads libgcc_s on first use, which is not safe inside a signal
        // handler, so warm it up here.
        let mut warm = [std::ptr::null_mut::<libc::c_void>(); 2];
        unsafe { libc::backtrace(warm.as_mut_ptr(), 2) };
        for (i, signal) in SIGNALS.into_iter().enumerate() {
            unsafe {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = handler as *const () as usize;
                action.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
                libc::sigemptyset(&mut action.sa_mask);
                let previous = &raw mut PREVIOUS;
                libc::sigaction(signal, &action, (*previous)[i].as_mut_ptr());
            }
        }
    }
}

/// Install the fatal-signal reporter (a no-op where it is not supported).
pub fn install() {
    #[cfg(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos"))]
    imp::install();
}
