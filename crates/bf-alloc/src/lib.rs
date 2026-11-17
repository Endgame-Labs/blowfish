//! The process allocator.
//!
//! glibc malloc keeps freed memory in per-thread arenas, so a server that builds large indexes on
//! worker threads holds several times its live heap as resident memory. Linking this crate installs
//! jemalloc with background threads that return unused pages to the OS within a few seconds, and
//! exposes jemalloc's own accounting so resident memory can be split into live heap, allocator
//! overhead, and cached pages.
//!
//! Tuning lives in the `malloc_conf` below; `_RJEM_MALLOC_CONF` overrides it at run time.

#[cfg(not(target_env = "msvc"))]
mod imp {
    use tikv_jemalloc_ctl::{epoch, stats};
    use tikv_jemallocator::Jemalloc;

    #[global_allocator]
    static GLOBAL: Jemalloc = Jemalloc;

    /// Background purge threads, an 8-arena cap (default is 4 per CPU, which fragments small boxes),
    /// and 1 s decay for dirty and muzzy pages so freed build buffers leave RSS quickly.
    #[unsafe(export_name = "_rjem_malloc_conf")]
    pub static MALLOC_CONF: &[u8] =
        b"background_thread:true,narenas:4,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

    pub fn stats() -> Option<super::Stats> {
        epoch::advance().ok()?;
        Some(super::Stats {
            allocated: stats::allocated::read().ok()? as u64,
            active: stats::active::read().ok()? as u64,
            resident: stats::resident::read().ok()? as u64,
            retained: stats::retained::read().ok()? as u64,
        })
    }
}

#[cfg(target_env = "msvc")]
mod imp {
    pub fn stats() -> Option<super::Stats> {
        None
    }
}

/// jemalloc's accounting, in bytes.
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    /// Bytes the program has allocated and not freed (the live heap).
    pub allocated: u64,
    /// Bytes in pages with at least one live allocation (live heap plus size-class rounding and
    /// partly used pages).
    pub active: u64,
    /// Bytes in physically resident pages the allocator maps (active plus not-yet-purged dirty pages).
    pub resident: u64,
    /// Virtual bytes retained for reuse that are not resident.
    pub retained: u64,
}

/// Current allocator statistics, or `None` where jemalloc is not in use.
pub fn stats() -> Option<Stats> {
    imp::stats()
}

/// Mirror the statistics into the `metrics` gauges `blowfish_alloc_bytes{kind}`.
pub fn publish_gauges() {
    if let Some(s) = stats() {
        for (kind, v) in [
            ("allocated", s.allocated),
            ("active", s.active),
            ("resident", s.resident),
            ("retained", s.retained),
        ] {
            metrics::gauge!("blowfish_alloc_bytes", "kind" => kind).set(v as f64);
        }
    }
}

/// Start the optional background monitor, driven by the environment:
/// - `BF_MEM_LOG_SECS=N` prints one line of allocator statistics and process RSS to stderr every N seconds.
/// - `BF_HEAP_DUMP_DIR=DIR` (with `BF_HEAP_DUMP_SECS=N`, default 30) writes a jemalloc heap profile
///   `DIR/heap.<seq>.heap` every N seconds. It needs the `heap-profile` feature and
///   `_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19`; read them with `jeprof`.
pub fn start_monitor() {
    let secs = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
    };
    if let Some(n) = secs("BF_MEM_LOG_SECS") {
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(n));
                let rss = std::fs::read_to_string("/proc/self/statm")
                    .ok()
                    .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
                    .map_or(0, |pages| pages * 4096);
                let mb = |b: u64| b >> 20;
                match stats() {
                    Some(s) => eprintln!(
                        "mem t={}s rss={}MB allocated={}MB active={}MB resident={}MB retained={}MB",
                        start.elapsed().as_secs(),
                        mb(rss),
                        mb(s.allocated),
                        mb(s.active),
                        mb(s.resident),
                        mb(s.retained)
                    ),
                    None => eprintln!("mem t={}s rss={}MB", start.elapsed().as_secs(), mb(rss)),
                }
            }
        });
    }
    #[cfg(all(feature = "heap-profile", not(target_env = "msvc")))]
    if let Ok(dir) = std::env::var("BF_HEAP_DUMP_DIR") {
        let n = secs("BF_HEAP_DUMP_SECS").unwrap_or(30);
        std::thread::spawn(move || {
            for seq in 0u64.. {
                std::thread::sleep(std::time::Duration::from_secs(n));
                let path = std::ffi::CString::new(format!("{dir}/heap.{seq:05}.heap"))
                    .expect("dump path has no NUL");
                // SAFETY: `prof.dump` takes a NUL-terminated path pointer.
                let r = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path.as_ptr()) };
                if let Err(e) = r {
                    eprintln!("heap dump failed: {e}");
                    return;
                }
            }
        });
    }
}
