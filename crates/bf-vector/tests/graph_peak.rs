//! Round GM1: `HotGraph::reservation` must match the real construction peak. A counting global
//! allocator records the peak of live heap bytes while a graph is built, and the reservation's
//! per-row cost (its slope in the row count, margin removed) must sit within 3% of it.
use bf_vector::warm_graph::{HotGraph, HotGraphBuilder};
use half::f16;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}
// SAFETY: every call forwards to the system allocator unchanged; only counters are updated.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        grow(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        grow(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if new >= l.size() {
            grow(new - l.size());
        } else {
            LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, new) }
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn reservation_slope_matches_the_measured_construction_peak() {
    let (dims, n) = (256usize, 30_000usize);
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let mut builder = HotGraphBuilder::new(dims, n).unwrap();
    let mut state = 12345u64;
    for start in (0..n).step_by(1000) {
        let rows = (start..start + 1000).map(|r| r as u32).collect::<Vec<_>>();
        let packed = (0..1000 * dims)
            .flat_map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                f16::from_f32((state % 2001) as f32 / 1000. - 1.).to_le_bytes()
            })
            .collect::<Vec<_>>();
        builder.append(&rows, &packed, &|| false).unwrap();
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let graph = pool.install(|| builder.finish(&|| false)).unwrap();
    let measured = PEAK.load(Ordering::Relaxed) - base;
    // The slope of the reservation, with its 1/16 margin removed, is the modeled bytes per row.
    let (r1, r2) = (
        HotGraph::reservation(n, dims, 1).unwrap(),
        HotGraph::reservation(2 * n, dims, 1).unwrap(),
    );
    let modeled_per_row = (r2 - r1) as f64 / n as f64 * 16. / 17.;
    let measured_per_row = measured as f64 / n as f64;
    eprintln!("per row: modeled {modeled_per_row:.1}, measured {measured_per_row:.1}");
    assert!(
        measured_per_row <= modeled_per_row * 1.01,
        "model undercounts the peak"
    );
    assert!(
        measured_per_row >= modeled_per_row * 0.97,
        "model overcounts the peak"
    );
    assert!(measured < HotGraph::reservation(n, dims, 2).unwrap() as usize);
    // The retained graph is what the shrunken reservation must still cover.
    assert!((graph.resident_bytes() as usize) < measured);
}
