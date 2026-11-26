//! Round G12 experiment switches. Temporary: each lets one process measure an optimization
//! against the old path on the same promoted graph. They are deleted once the winners ship.
use std::sync::atomic::{AtomicU32, Ordering};

/// Bits set at run time by the server (from a flag file); zero is the old behavior everywhere.
pub static FLAGS: AtomicU32 = AtomicU32::new(0);
/// Item 1: graph queries above 10 rows stay on the HTTP reactor.
pub const INLINE_BIG: u32 = 1;
/// Item 2: sample the per-query CPU clock instead of reading it for every query.
pub const SAMPLED_CPU: u32 = 2;
/// Item 3: one RaBitQ transform per thread, applied without allocation.
pub const ROTATE_ONCE: u32 = 4;
/// Item 4: rerank straight from the fetched bytes with a fused f16 dot.
pub const FUSED_RERANK: u32 = 8;
/// Item 5: carry `(segment, row, distance)` through the merge.
pub const LATE_IDS: u32 = 16;

/// Whether the given switch is on.
#[inline]
pub fn on(bit: u32) -> bool {
    FLAGS.load(Ordering::Relaxed) & bit != 0
}
