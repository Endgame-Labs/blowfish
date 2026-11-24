//! Optional perf-stat FIFO control: count only the resident query region, excluding load/build.
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    time::Instant,
};
pub struct Region {
    start: Instant,
    perf: Option<(File, BufReader<File>)>,
    #[cfg(target_os = "macos")]
    counters: Option<(u64, u64, u64)>,
}
impl Region {
    pub fn begin() -> Self {
        let perf = std::env::var_os("G3_PERF_CTL").map(|ctl| {
            let control = OpenOptions::new()
                .write(true)
                .open(ctl)
                .expect("perf control FIFO");
            let ack = File::open(std::env::var_os("G3_PERF_ACK").expect("perf ack path"))
                .expect("perf ack FIFO");
            (control, BufReader::new(ack))
        });
        let mut region = Self {
            start: Instant::now(),
            perf,
            #[cfg(target_os = "macos")]
            counters: None,
        };
        region.command("enable\n");
        #[cfg(target_os = "macos")]
        {
            region.counters = task_counters();
        }
        region.start = Instant::now();
        region
    }
    fn command(&mut self, s: &str) {
        if let Some((control, ack)) = &mut self.perf {
            control.write_all(s.as_bytes()).unwrap();
            control.flush().unwrap();
            let mut line = String::new();
            ack.read_line(&mut line).unwrap();
            assert!(line.trim_start_matches('\0').starts_with("ack"));
        }
    }
    pub fn end(mut self) -> f64 {
        let seconds = self.start.elapsed().as_secs_f64();
        #[cfg(target_os = "macos")]
        if let (Some(before), Some(after)) = (self.counters, task_counters()) {
            eprintln!(
                "task_region instructions={} cycles={} rss_kib={}",
                after.0 - before.0,
                after.1 - before.1,
                after.2 / 1024
            );
        }
        self.command("disable\n");
        seconds
    }
}

/// Give each Rayon worker one reusable state and a strided query stream. Repeated passes do
/// not begin at the same query on every worker, and non-power-of-two pools use every worker.
pub fn parallel<S: Send, R: Send>(
    count: usize,
    init: impl Fn() -> S + Sync,
    run: impl Fn(&mut S, usize) -> R + Sync,
) -> Vec<R> {
    use rayon::prelude::*;
    let threads = rayon::current_num_threads();
    (0..threads)
        .into_par_iter()
        .map(|worker| {
            let mut state = init();
            (worker..count)
                .step_by(threads)
                .map(|i| run(&mut state, i))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .into_iter()
        .flatten()
        .collect()
}
#[cfg(target_os = "macos")]
fn task_counters() -> Option<(u64, u64, u64)> {
    #[link(name = "proc")]
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut std::ffi::c_void) -> i32;
    }
    let mut values = [0u64; 64];
    // SAFETY: the stable RUSAGE_INFO_V4 ABI requires an eight-byte-aligned, 296-byte output
    // structure. This buffer is larger, and instruction/cycle fields are words 31 and 32.
    let status =
        unsafe { proc_pid_rusage(std::process::id() as i32, 4, values.as_mut_ptr().cast()) };
    (status == 0).then_some((values[31], values[32], values[8]))
}
#[cfg(test)]
mod tests {
    #[test]
    fn non_power_of_two_jobs_cover_each_query_once_with_reused_state() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap();
        let result = pool.install(|| {
            super::parallel(
                13,
                || 0usize,
                |state, i| {
                    *state += 1;
                    (i, *state)
                },
            )
        });
        let mut ids = result.iter().map(|r| r.0).collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (0..13).collect::<Vec<_>>());
        assert_eq!(result.iter().filter(|r| r.1 == 1).count(), 3);
        assert_eq!(result.iter().map(|r| r.1).max(), Some(5));
    }
}
