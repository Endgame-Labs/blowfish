//! Disposable, bounded-memory row graphs for warm immutable cosine-f16 tiers.
//!
//! Source rows fill the final code arrays directly. Construction retains global SQ8
//! and fixed-capacity adjacency, without a full f16 corpus or spatial partitions. Nothing here
//! defines a bucket format. Construction runs on the caller's rayon pool.
mod binary;
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
    norms: Vec<crate::CosineNorm>,
    /// Graph node of each tier row (`u32::MAX` where the tier row is not a node), or empty when
    /// the tier's row numbers are too sparse for a dense table. Serves [`HotGraph::scan`].
    nodes: Vec<u32>,
}
/// Reusable per-query exact visitation and bounded beam.
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
            + self.codes.scales.capacity() * 4
            + self.norms.capacity() * 4
            + self.nodes.capacity() * 4) as u64
    }
    /// Conservative construction/load reservation, excluding source-index metadata.
    /// `workers` is the maximum number of simultaneous query scratch arrays.
    pub fn reservation(rows: usize, dims: usize, workers: usize) -> Option<u64> {
        if rows == 0 || rows > u32::MAX as usize || dims == 0 || dims > 4096 {
            return None;
        }
        vamana::reservation(rows as u64, dims.next_power_of_two() as u64, workers as u64)
    }
    /// `bytes` plus the one stated safety margin (1/16) every graph reservation carries.
    pub fn with_margin(bytes: u64) -> u64 {
        vamana::with_margin(bytes)
    }
    /// Number of source rows still eligible in the supplied tier mask.
    pub fn live_count(&self, mask: &RoaringBitmap) -> usize {
        self.rows.iter().filter(|row| mask.contains(**row)).count()
    }
    /// Whether [`Self::scan`] can serve this graph (its tier row numbers are dense enough).
    pub fn can_scan(&self) -> bool {
        !self.nodes.is_empty()
    }
    /// Score only the rows of `allowed` against the SQ8 codes, and return the `count` nearest
    /// by that score (ascending, ties by tier row) with their cached norms for exact reranking,
    /// plus the number of rows scored. Rows of `allowed` that are not graph nodes are skipped.
    pub fn scan(
        &self,
        query: &[f32],
        count: usize,
        allowed: &RoaringBitmap,
    ) -> Result<(Vec<(u32, crate::CosineNorm)>, usize)> {
        crate::distance::validate(query, self.dims)?;
        if count == 0 || self.nodes.is_empty() || query.iter().all(|x| *x == 0.) {
            return Err(Error::InvalidInput("unsupported graph query shape".into()));
        }
        let q = vectors::normalized(query);
        let (code, scale) = self.codes.prepare(&q);
        let ids: Vec<u32> = allowed
            .iter()
            .filter_map(|row| self.nodes.get(row as usize).copied())
            .filter(|node| *node != u32::MAX)
            .collect();
        // Prefetching a fixed distance ahead overlaps the row-sized random reads.
        const AHEAD: usize = 24;
        let mut scored: Vec<(f32, u32)> = Vec::with_capacity(ids.len());
        for (i, &id) in ids.iter().enumerate() {
            if let Some(&next) = ids.get(i + AHEAD) {
                self.codes.prefetch(next);
            }
            scored.push((self.codes.score(&code, scale, id), id));
        }
        let order = |a: &(f32, u32), b: &(f32, u32)| {
            a.0.total_cmp(&b.0)
                .then(self.rows[a.1 as usize].cmp(&self.rows[b.1 as usize]))
        };
        if scored.len() > count {
            scored.select_nth_unstable_by(count - 1, order);
            scored.truncate(count);
        }
        scored.sort_unstable_by(order);
        let scanned = ids.len();
        Ok((
            scored
                .into_iter()
                .map(|(_, id)| (self.rows[id as usize], self.norms[id as usize]))
                .collect(),
            scanned,
        ))
    }
    /// Allocate cache-sized visitation state. Callers bound and reserve the number retained.
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
        let (hits, evaluations) = self.candidates_with_norms(query, ef, count, live, scratch)?;
        Ok((hits.into_iter().map(|(row, _)| row).collect(), evaluations))
    }
    /// Navigate identically to [`Self::candidates`], carrying each immutable row's cached norm
    /// for exact reranking. Norms describe original stored f16 values, before normalization.
    pub fn candidates_with_norms(
        &self,
        query: &[f32],
        ef: usize,
        count: usize,
        live: Option<&RoaringBitmap>,
        scratch: &mut HotScratch,
    ) -> Result<(Vec<(u32, crate::CosineNorm)>, usize)> {
        crate::distance::validate(query, self.dims)?;
        if ef == 0 || count == 0 || query.iter().all(|x| *x == 0.) {
            return Err(Error::InvalidInput("unsupported graph query shape".into()));
        }
        let q = vectors::normalized(query);
        let (code, scale) = self.codes.prepare(&q);
        let entry = self
            .entries
            .iter()
            .map(|&id| (id, self.codes.score(&code, scale, id)))
            .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
            .unwrap()
            .0;
        let (mut hits, evaluations) = scratch.0.search_prefetched(
            &self.links,
            &self.degrees,
            entry,
            ef.max(count).min(self.len()),
            |id| self.codes.score(&code, scale, id),
            |id| self.codes.prefetch(id),
        );
        hits.retain(|h| live.is_none_or(|m| m.contains(self.rows[h.id as usize])));
        hits.truncate(count);
        Ok((
            hits.into_iter()
                .map(|h| (self.rows[h.id as usize], self.norms[h.id as usize]))
                .collect(),
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
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.scratch")
            .join(format!(
                "g4-vector-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
    }
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn streaming_build_maps_rows_masks_dead_nodes_and_never_spills() {
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
        let mut b = HotGraphBuilder::new(dims, n).unwrap();
        for (part, rows) in source.chunks(40).enumerate() {
            let ids = (part * 40..part * 40 + rows.len())
                .map(|i| 1000 + i as u32 * 3)
                .collect::<Vec<_>>();
            let packed = rows
                .iter()
                .flatten()
                .flat_map(|x| f16::from_f32(*x).to_le_bytes())
                .collect::<Vec<_>>();
            b.append(&ids, &packed, &|| false).unwrap();
        }
        let graph = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| b.finish(&|| false))
            .unwrap();
        assert!(!root.exists());
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
        let direction = (0..dims)
            .map(|i| if i % 2 == 0 { 1. } else { -1. })
            .collect::<Vec<_>>();
        let expected = graph
            .candidates(&direction, n, 30, None, &mut scratch)
            .unwrap()
            .0;
        for scale in [f32::MAX, f32::from_bits(1)] {
            let query = direction.iter().map(|x| x * scale).collect::<Vec<_>>();
            assert_eq!(
                graph
                    .candidates(&query, n, 30, None, &mut scratch)
                    .unwrap()
                    .0,
                expected
            );
        }

        assert!(graph.resident_bytes() < HotGraph::reservation(n, dims, 2).unwrap());
    }
    #[test]
    fn scan_scores_only_allowed_rows_and_matches_a_brute_force_order() {
        let dims = 16;
        let n = 160;
        let mut seed = 5u64;
        let source = (0..n)
            .map(|_| {
                (0..dims)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        (seed >> 40) as f32 / 16777216. * 2. - 1.
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut b = HotGraphBuilder::new(dims, n).unwrap();
        // Tier rows are not node order: node i holds row (i * 7) % n + 5 (a permutation plus a gap).
        let row = |i: usize| ((i * 7) % n) as u32 + 5;
        let ids = (0..n).map(row).collect::<Vec<_>>();
        let packed = source
            .iter()
            .flatten()
            .flat_map(|x| f16::from_f32(*x).to_le_bytes())
            .collect::<Vec<_>>();
        b.append(&ids, &packed, &|| false).unwrap();
        let graph = b.finish(&|| false).unwrap();
        assert!(graph.can_scan());
        let query = &source[41];
        // Every third row, a single row, none of the graph's rows, and every row.
        let thirds = (0..n)
            .map(row)
            .filter(|r| r % 3 == 0)
            .collect::<RoaringBitmap>();
        let one = RoaringBitmap::from_iter([row(41)]);
        let outside = RoaringBitmap::from_iter([0, 1, 100_000]);
        let all = (0..n).map(row).collect::<RoaringBitmap>();
        for allowed in [&thirds, &one, &outside, &all] {
            let (hits, scanned) = graph.scan(query, 12, allowed).unwrap();
            let inside = allowed.iter().filter(|r| ids.contains(r)).count();
            assert_eq!(scanned, inside);
            assert_eq!(hits.len(), inside.min(12));
            assert!(hits.iter().all(|(r, _)| allowed.contains(*r)));
            // With the whole allowed set shortlisted, the scan equals a full ranking.
            let (full, _) = graph.scan(query, n, allowed).unwrap();
            assert_eq!(
                full[..hits.len()].iter().map(|h| h.0).collect::<Vec<_>>(),
                hits.iter().map(|h| h.0).collect::<Vec<_>>()
            );
        }
        // The nearest allowed row to a stored vector is that row when it is allowed.
        assert_eq!(graph.scan(query, 3, &all).unwrap().0[0].0, row(41));
        // Zero queries and zero counts are refused like graph navigation.
        assert!(graph.scan(&vec![0.; dims], 3, &all).is_err());
        assert!(graph.scan(query, 0, &all).is_err());
    }
    #[test]
    fn cancellation_and_invalid_input_leave_no_spills() {
        let root = root();
        let _cleanup = Cleanup(root.clone());
        let mut b = HotGraphBuilder::new(2, 1).unwrap();
        assert!(b.append(&[1], &[0; 4], &|| false).is_err());
        let p = [f16::ONE, f16::ZERO]
            .into_iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>();
        b.append(&[1], &p, &|| false).unwrap();
        let cancel = HotCancellation::default();
        cancel.cancel();
        assert!(b.finish(&|| cancel.cancelled()).is_err());
        assert!(!root.exists());
    }
}
