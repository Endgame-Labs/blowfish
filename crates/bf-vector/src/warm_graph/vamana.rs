//! Compact, disposable Vamana construction. Two passes, with reciprocal robust pruning.
//! Locks protect only adjacency; a search copies one list before releasing its read lock.
//! Construction searches with the same beam core as serving, and prunes from the distances
//! that search already computed.
use super::{
    navigation::{Hit, Scratch},
    quant::Codes,
};
use parking_lot::RwLock;
use rayon::prelude::*;

/// Reusable per-thread construction state. Nothing here allocates after warm-up.
struct Work {
    search: Scratch,
    /// Nodes the last search expanded, then their exact distances from the point being inserted.
    expanded: Vec<Hit>,
    /// Candidate pool being pruned, sorted by distance from the center.
    pool: Vec<Hit>,
    picked: Vec<u32>,
    /// A copy of the point's new neighbors, which receive reverse edges while `picked` is reused.
    targets: Vec<u32>,
}
impl Work {
    fn new(n: usize) -> Self {
        Self {
            search: Scratch::new(n),
            expanded: Vec::with_capacity(256),
            pool: Vec::with_capacity(256),
            picked: Vec::with_capacity(DEGREE * 2),
            targets: Vec::with_capacity(DEGREE * 2),
        }
    }
    /// Robust-prune `self.pool` (distances already measured from `center`) into `self.picked`.
    fn prune(&mut self, v: &Codes, center: u32, degree: usize, alpha: f32) {
        self.pool.retain(|p| p.id != center);
        self.pool.sort_unstable();
        self.picked.clear();
        while !self.pool.is_empty() && self.picked.len() < degree {
            let picked = self.pool[0];
            self.picked.push(picked.id);
            self.pool
                .retain(|p| p.id != picked.id && alpha * v.between(picked.id, p.id) > p.distance);
        }
    }
}
pub(super) const DEGREE: usize = 32;
/// Construction beam width, steered by one-bit Hamming distance. Round GB1 measured this
/// matching or beating exact SQ8 steering at the same width on recall, with far fewer row reads.
const BEAM: usize = 64;
#[derive(Clone, Copy)]
pub(super) struct Neighbors {
    ids: [u32; DEGREE * 2],
    len: u8,
}
impl Neighbors {
    fn empty() -> Self {
        Self {
            ids: [0; DEGREE * 2],
            len: 0,
        }
    }
    pub(super) fn as_slice(&self) -> &[u32] {
        &self.ids[..self.len as usize]
    }
    fn replace(&mut self, ids: &[u32]) {
        assert!(ids.len() <= self.ids.len());
        self.ids[..ids.len()].copy_from_slice(ids);
        self.len = ids.len() as u8;
    }
    fn push(&mut self, id: u32) {
        self.ids[self.len as usize] = id;
        self.len += 1;
    }
}
/// Fixed allowance on top of the per-row peak: per-thread visitation bitsets and construction
/// scratch for the largest build pool (six threads), the beam and neighbor scratch, and allocator
/// slack. Round GM1 measured the real fixed part far below it.
const FIXED: u64 = 64 << 20;
/// Safety margin on the per-row peak, as a fraction: 1/16, covering allocator rounding of the
/// large arrays and the capacities the build's vectors grow to. Round GM1 measured the true peak
/// within 1% of the per-row sum below, so this margin is real headroom, not a fudge.
const MARGIN_DIVISOR: u64 = 16;

/// Exact bytes per row at the construction peak. While `build` runs the graph holds the SQ8 codes
/// and scale, the tier row coordinate and cosine norm, the build-time adjacency (`RwLock<Neighbors>`),
/// the one-bit codes, and the insertion permutation. The one-bit codes stay resident (filtered scans
/// screen with them), so the publication peak (build adjacency, flat links, and one-bit codes
/// together) is the other candidate; the larger of the two is the peak.
pub(super) fn peak_bytes_per_row(padded: u64) -> Option<u64> {
    let lock = std::mem::size_of::<RwLock<Neighbors>>() as u64;
    // Codes (`padded` bytes) and scale (4), tier row (4), and norm (4).
    let base = padded.checked_add(4 + 4 + 4)?;
    let building = base
        .checked_add(lock)?
        .checked_add(padded.div_ceil(8).next_multiple_of(8))?
        .checked_add(4)?;
    // Flat links (34 ids of 4 bytes) and the degree byte, while the build adjacency still exists,
    // plus the one-bit codes, which stay resident for filtered scans.
    let publication = base
        .checked_add(lock)?
        .checked_add(34 * 4 + 1)?
        .checked_add(padded.div_ceil(8).next_multiple_of(8))?;
    Some(building.max(publication))
}
/// Bytes per row that the finished graph retains, as `HotGraph::resident_bytes` counts them.
fn retained_bytes_per_row(padded: u64) -> Option<u64> {
    // Codes, scale, tier row, norm, flat links and degree, and the resident one-bit codes.
    padded
        .checked_add(4 + 4 + 4 + 34 * 4 + 1)?
        .checked_add(padded.div_ceil(8).next_multiple_of(8))
}
/// Reservation for building and then serving a graph: the construction peak with a stated 1/16
/// margin, or the retained graph with `queries` simultaneous visitation arrays, whichever is
/// larger. Round GM1 replaced a 25% surcharge on a doubled per-row estimate by this exact
/// breakdown, which a test checks against a counting allocator.
pub(super) fn reservation(rows: u64, padded: u64, queries: u64) -> Option<u64> {
    let construction = rows
        .checked_mul(peak_bytes_per_row(padded)?)?
        .checked_add(FIXED)?;
    let serving = rows
        .checked_mul(retained_bytes_per_row(padded)?)?
        .checked_add(
            queries.checked_mul(rows.div_ceil(32).checked_mul(8)?.checked_add(2 << 20)?)?,
        )?;
    construction
        .max(serving)
        .checked_add(construction.max(serving) / MARGIN_DIVISOR)
}
/// `bytes` with the stated 1/16 margin.
pub(super) fn with_margin(bytes: u64) -> u64 {
    bytes.saturating_add(bytes / MARGIN_DIVISOR)
}
pub fn build(
    v: &Codes,
    bits: &super::binary::Bits,
    entry: u32,
    cancelled: &(impl Fn() -> bool + Sync),
) -> crate::Result<Vec<RwLock<Neighbors>>> {
    let n = v.scales.len();
    let degree = DEGREE.min(n.saturating_sub(1));
    let mut links = Vec::with_capacity(n);
    for i in 0..n {
        super::check(cancelled)?;
        let mut seed = i as u64 + 7;
        let mut neighbors = Neighbors::empty();
        while neighbors.as_slice().len() < degree {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let id = (seed % n as u64) as u32;
            if id != i as u32 && !neighbors.as_slice().contains(&id) {
                neighbors.push(id);
            }
        }
        links.push(RwLock::new(neighbors));
    }
    let mut ids = (0..n as u32).collect::<Vec<_>>();
    let mut seed = 7u64;
    for i in (1..n).rev() {
        if i.is_multiple_of(4096) {
            super::check(cancelled)?;
        }
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        ids.swap(i, seed as usize % (i + 1));
    }
    for alpha in [1., 1.2] {
        ids.par_iter().for_each_init(
            || Work::new(n),
            |w, &point| {
                if cancelled() {
                    return;
                }
                w.expanded.clear();
                let Work {
                    search, expanded, ..
                } = w;
                let own = bits.row(point);
                search.search_with(
                    |id, out| {
                        let row = links[id as usize].read();
                        let row = row.as_slice();
                        out[..row.len()].copy_from_slice(row);
                        row.len()
                    },
                    entry,
                    BEAM,
                    |id| bits.distance(own, id),
                    |id| bits.prefetch(id),
                    |hit| expanded.push(hit),
                    None,
                    None,
                );
                // Steering used Hamming distance; the prune test needs true distances. Only the
                // expanded nodes are measured, a small fraction of everything the search scored.
                for i in 0..w.expanded.len() {
                    if let Some(next) = w.expanded.get(i + 4) {
                        v.prefetch(next.id);
                    }
                    let id = w.expanded[i].id;
                    w.expanded[i].distance = v.between(point, id);
                }
                let mut current = links[point as usize].write();
                // Candidates are unique, so only current neighbors they miss cost another score.
                w.pool.clear();
                w.pool.extend_from_slice(&w.expanded);
                w.expanded.sort_unstable_by_key(|h| h.id);
                for &id in current.as_slice() {
                    if w.expanded.binary_search_by_key(&id, |h| h.id).is_err() {
                        w.pool.push(Hit {
                            id,
                            distance: v.between(point, id),
                        });
                    }
                }
                w.prune(v, point, degree, alpha);
                current.replace(&w.picked);
                drop(current);
                w.targets.clear();
                w.targets.extend_from_slice(&w.picked);
                for target in 0..w.targets.len() {
                    let neighbor = w.targets[target];
                    let mut incoming = links[neighbor as usize].write();
                    if incoming.as_slice().contains(&point) {
                        continue;
                    }
                    if incoming.as_slice().len() < degree * 2 {
                        incoming.push(point);
                    } else {
                        w.pool.clear();
                        w.pool
                            .extend(incoming.as_slice().iter().chain([&point]).map(|&id| Hit {
                                id,
                                distance: v.between(neighbor, id),
                            }));
                        w.prune(v, neighbor, degree, alpha);
                        incoming.replace(&w.picked);
                    }
                }
            },
        );
        super::check(cancelled)?;
        links.par_iter().enumerate().for_each_init(
            || Work::new(0),
            |w, (point, row)| {
                if cancelled() {
                    return;
                }
                let mut row = row.write();
                if row.as_slice().len() > degree {
                    w.pool.clear();
                    w.pool.extend(row.as_slice().iter().map(|&id| Hit {
                        id,
                        distance: v.between(point as u32, id),
                    }));
                    w.prune(v, point as u32, degree, alpha);
                    row.replace(&w.picked);
                }
            },
        );
        super::check(cancelled)?;
    }
    Ok(links)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn fixed_adjacency_cancels_mid_build_and_finishes_at_degree_bound() {
        let dims = 16;
        let n = 600;
        let transform = crate::rabitq::RaBitQ::new(dims, 42).unwrap();
        let mut codes = Codes {
            dims,
            values: Vec::new(),
            scales: Vec::new(),
            dot: super::super::quant::dot_kernel(),
        };
        for id in 0..n {
            let row = (0..dims)
                .map(|i| ((id * 29 + i * 37) as f32 * 0.127).sin())
                .collect::<Vec<_>>();
            let row = super::super::vectors::normalized(&row);
            let (code, scale) = super::super::quant::encode(&transform.rotate(&row));
            codes.values.extend(code);
            codes.scales.push(scale);
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let checks = AtomicUsize::new(0);
        let bits = super::super::binary::Bits::new(&codes, &|| false).unwrap();
        let result = pool.install(|| {
            build(&codes, &bits, 0, &|| {
                checks.fetch_add(1, Ordering::Relaxed) >= 2000
            })
        });
        assert!(
            matches!(result, Err(crate::Error::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted)
        );
        assert!(checks.load(Ordering::Relaxed) >= 2000);
        let nodes = pool.install(|| build(&codes, &bits, 0, &|| false)).unwrap();
        assert_eq!(nodes.len(), n);
        assert!(std::mem::size_of::<RwLock<Neighbors>>() <= 280);
        for (id, row) in nodes.into_iter().enumerate() {
            let row = row.into_inner();
            assert!(row.as_slice().len() <= DEGREE);
            assert!(
                row.as_slice()
                    .iter()
                    .all(|x| *x < n as u32 && *x != id as u32)
            );
            let mut unique = row.as_slice().to_vec();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(unique.len(), row.as_slice().len());
        }
    }
}
