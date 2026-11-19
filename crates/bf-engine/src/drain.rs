//! How fast the indexer drains the unindexed WAL, so a backpressure 429 can say when to retry.
use std::sync::Mutex;
use std::time::Duration;

/// Smoothed indexing throughput in bytes per second (0 before the first publish).
static RATE: Mutex<f64> = Mutex::new(0.0);
const MIN_RETRY_MS: u64 = 500;
const MAX_RETRY_MS: u64 = 30_000;
const UNKNOWN_RETRY_MS: u64 = 1_000;

/// Record one publish: the indexer cleared `bytes` of tail in `elapsed`.
pub(crate) fn record(bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    if bytes == 0 || secs <= 0.0 {
        return;
    }
    let sample = bytes as f64 / secs;
    let mut rate = RATE.lock().unwrap_or_else(|e| e.into_inner());
    *rate = if *rate == 0.0 {
        sample
    } else {
        *rate * 0.7 + sample * 0.3
    };
}

/// Retry delay for a write rejected with `tail_bytes` unindexed: the time the indexer needs to
/// clear that backlog at its recent rate, clamped to 0.5 to 30 s (1 s before any measurement).
pub(crate) fn retry_after_ms(tail_bytes: u64) -> u64 {
    let rate = *RATE.lock().unwrap_or_else(|e| e.into_inner());
    estimate(tail_bytes, rate)
}

fn estimate(tail_bytes: u64, rate: f64) -> u64 {
    if rate <= 0.0 {
        return UNKNOWN_RETRY_MS;
    }
    ((tail_bytes as f64 / rate * 1000.0) as u64).clamp(MIN_RETRY_MS, MAX_RETRY_MS)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn estimates_are_clamped() {
        assert_eq!(estimate(100, 0.0), 1_000);
        assert_eq!(estimate(1, 1e9), 500);
        assert_eq!(estimate(1 << 30, 1e6), 30_000);
        assert_eq!(estimate(10_000_000, 5_000_000.0), 2_000);
    }
}
