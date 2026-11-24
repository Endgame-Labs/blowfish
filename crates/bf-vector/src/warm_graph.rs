//! Disposable, bounded-memory row graphs for warm immutable cosine-f16 tiers.
//!
//! Source rows and partial adjacency spill to caller-owned local storage. Nothing here defines
//! a bucket format. Construction uses the caller's CPU pool and checks cancellation between rows.
mod builder;
mod navigation;
mod quant;
mod vamana;
mod vectors;
use crate::{Error, Result, RoaringBitmap, rabitq};
pub use builder::HotGraphBuilder;
use std::sync::atomic::{AtomicBool, Ordering};

fn check(cancelled: &(impl Fn() -> bool + Sync)) -> Result<()> {
    if cancelled() {
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "graph construction cancelled",
        )))
    } else {
        Ok(())
    }
}
/// Immutable graph, rotated SQ8 codes, and original tier-row coordinates.
pub struct HotGraph {
    dims: usize,
    rows: Vec<u32>,
    links: Vec<[u32; 34]>,
    degrees: Vec<u8>,
    entries: Vec<u32>,
    codes: quant::Codes,
}
/// Reusable per-query visited bytes and bounded heaps.
pub struct HotScratch(navigation::Scratch);
impl HotGraph {
    /// Number of graph nodes.
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    /// Whether this graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// Original vector dimensions, before rotation padding.
    pub fn dims(&self) -> usize {
        self.dims
    }
    /// Capacities retained by the graph, excluding allocator overhead and query scratch.
    pub fn resident_bytes(&self) -> u64 {
        (self.rows.capacity() * 4
            + self.links.capacity() * 136
            + self.degrees.capacity()
            + self.entries.capacity() * 4
            + self.codes.values.capacity()
            + self.codes.scales.capacity() * 4) as u64
    }
    /// Conservative construction/load reservation, excluding source-index metadata.
    /// `workers` is the maximum number of simultaneous query scratch arrays.
    pub fn reservation(rows: usize, dims: usize, workers: usize) -> Option<u64> {
        if rows == 0 || rows > u32::MAX as usize || dims == 0 || dims > 4096 {
            return None;
        }
        let resident =
            (rows as u64).checked_mul((dims.next_power_of_two() + 160 + workers) as u64)?;
        // Includes the capped partition's packed input, decoded f16, codes, links, and heaps.
        resident
            .checked_add(512 << 20)?
            .checked_mul(5)
            .map(|n| n / 4)
    }
    /// Number of source rows still eligible in the supplied tier mask.
    pub fn live_count(&self, mask: &RoaringBitmap) -> usize {
        self.rows.iter().filter(|row| mask.contains(**row)).count()
    }
    /// Allocate a byte-per-node visited array. Callers bound and reserve the number retained.
    pub fn scratch(&self) -> HotScratch {
        HotScratch(navigation::Scratch::new(self.len()))
    }
    /// Navigate SQ8 codes, returning original tier rows and the number of distance evaluations.
    /// Dead rows remain traversable and are removed before the exact-rerank pool is selected.
    pub fn candidates(
        &self,
        query: &[f32],
        ef: usize,
        count: usize,
        live: Option<&RoaringBitmap>,
        scratch: &mut HotScratch,
    ) -> Result<(Vec<u32>, usize)> {
        crate::distance::validate(query, self.dims)?;
        if ef == 0 || count == 0 || query.iter().all(|x| *x == 0.) {
            return Err(Error::InvalidInput("unsupported graph query shape".into()));
        }
        let q = vectors::normalized(query);
        let (code, scale) = self.codes.prepare(&q);
        let entry = *self
            .entries
            .iter()
            .min_by(|a, b| {
                self.codes
                    .score(&code, scale, **a)
                    .total_cmp(&self.codes.score(&code, scale, **b))
            })
            .unwrap();
        let (mut hits, evaluations) = scratch.0.search(
            &self.links,
            &self.degrees,
            entry,
            ef.max(count).min(self.len()),
            |id| self.codes.score(&code, scale, id),
        );
        hits.retain(|h| live.is_none_or(|m| m.contains(self.rows[h.id as usize])));
        hits.truncate(count);
        Ok((
            hits.into_iter().map(|h| self.rows[h.id as usize]).collect(),
            evaluations + self.entries.len(),
        ))
    }
}
/// A cancellation flag usable by source streaming and CPU construction alike.
#[derive(Clone, Default)]
pub struct HotCancellation(std::sync::Arc<AtomicBool>);
impl HotCancellation {
    /// Cancel this build. Cancellation is irreversible.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    /// Whether cancellation has been requested.
    pub fn cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use half::f16;
    fn root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.scratch")
            .join(format!(
                "g4-vector-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
    }
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn spilled_build_maps_rows_masks_dead_nodes_and_cleans_up() {
        let root = root();
        let _cleanup = Cleanup(root.clone());
        let dims = 16;
        let n = 160;
        let source = (0..n)
            .map(|id| {
                (0..dims)
                    .map(|d| ((id * 19 + d * 37) as f32 * 0.127).sin())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut b = HotGraphBuilder::new(&root, dims, n).unwrap();
        for (part, rows) in source.chunks(40).enumerate() {
            let ids = (part * 40..part * 40 + rows.len())
                .map(|i| 1000 + i as u32 * 3)
                .collect::<Vec<_>>();
            let packed = rows
                .iter()
                .flatten()
                .flat_map(|x| f16::from_f32(*x).to_le_bytes())
                .collect::<Vec<_>>();
            b.append(part, &ids, &packed).unwrap();
        }
        let graph = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| b.finish(&|| false))
            .unwrap();
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        let mut scratch = graph.scratch();
        let live = (0..n)
            .filter(|i| *i != 7)
            .map(|i| 1000 + i as u32 * 3)
            .collect::<RoaringBitmap>();
        let (hits, _) = graph
            .candidates(&source[7], n, n, Some(&live), &mut scratch)
            .unwrap();
        assert_eq!(hits.len(), n - 1);
        assert!(hits.iter().all(|id| live.contains(*id)));
        let (hits, _) = graph
            .candidates(&source[51], n, 10, None, &mut scratch)
            .unwrap();
        // SQ8 navigation supplies candidates; exact storage rerank chooses the winner.
        assert!(hits.contains(&(1000 + 51 * 3)));
        assert!(graph.resident_bytes() < HotGraph::reservation(n, dims, 2).unwrap());
    }
    #[test]
    fn cancellation_and_invalid_input_leave_no_spills() {
        let root = root();
        let _cleanup = Cleanup(root.clone());
        let mut b = HotGraphBuilder::new(&root, 2, 1).unwrap();
        assert!(b.append(0, &[1], &[0; 4]).is_err());
        let p = [f16::ONE, f16::ZERO]
            .into_iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>();
        b.append(0, &[1], &p).unwrap();
        let cancel = HotCancellation::default();
        cancel.cancel();
        assert!(b.finish(&|| cancel.cancelled()).is_err());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    }
}
