//! Compact, disposable Vamana construction. Two passes, with reciprocal robust pruning.
//! Locks protect only adjacency; a search copies one list before releasing its read lock.
use super::quant::Codes;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};
#[derive(Clone, Copy, Debug)]
struct Hit {
    id: u32,
    distance: f32,
}
impl PartialEq for Hit {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for Hit {}
impl PartialOrd for Hit {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Hit {
    fn cmp(&self, o: &Self) -> Ordering {
        self.distance
            .total_cmp(&o.distance)
            .then(self.id.cmp(&o.id))
    }
}
struct Scratch {
    marks: Vec<u8>,
    generation: u8,
    frontier: BinaryHeap<Reverse<Hit>>,
    nearest: BinaryHeap<Hit>,
}
impl Scratch {
    fn new(n: usize) -> Self {
        Self {
            marks: vec![0; n],
            generation: 0,
            frontier: BinaryHeap::new(),
            nearest: BinaryHeap::new(),
        }
    }
    fn visit(&mut self, id: u32) -> bool {
        let p = &mut self.marks[id as usize];
        if *p == self.generation {
            false
        } else {
            *p = self.generation;
            true
        }
    }
    fn search(
        &mut self,
        v: &Codes,
        links: &[RwLock<Neighbors>],
        entry: u32,
        q: u32,
        beam: usize,
    ) -> Vec<u32> {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.marks.fill(0);
            self.generation = 1;
        }
        self.frontier.clear();
        self.nearest.clear();
        self.visit(entry);
        let start = Hit {
            id: entry,
            distance: v.between(q, entry),
        };
        self.frontier.push(Reverse(start));
        self.nearest.push(start);
        let mut expanded = Vec::with_capacity(beam * 2);
        while let Some(Reverse(current)) = self.frontier.pop() {
            if self.nearest.len() >= beam && current > *self.nearest.peek().unwrap() {
                break;
            }
            expanded.push(current.id);
            let neighbors = *links[current.id as usize].read();
            for &id in neighbors.as_slice() {
                if !self.visit(id) {
                    continue;
                }
                let h = Hit {
                    id,
                    distance: v.between(q, id),
                };
                if self.nearest.len() < beam || h < *self.nearest.peek().unwrap() {
                    self.frontier.push(Reverse(h));
                    self.nearest.push(h);
                    if self.nearest.len() > beam {
                        self.nearest.pop();
                    }
                    if self.frontier.len() > beam * 4 {
                        let threshold = *self.nearest.peek().unwrap();
                        self.frontier.retain(|h| h.0 <= threshold);
                    }
                }
            }
        }
        expanded
    }
}
pub fn prune(
    center: u32,
    mut candidates: Vec<u32>,
    degree: usize,
    alpha: f32,
    mut distance: impl FnMut(u32, u32) -> f32,
) -> Vec<u32> {
    candidates.sort_unstable();
    candidates.dedup();
    candidates.retain(|p| *p != center);
    let mut pool = candidates
        .into_iter()
        .map(|id| Hit {
            id,
            distance: distance(center, id),
        })
        .collect::<Vec<_>>();
    pool.sort_unstable();
    let mut out = Vec::with_capacity(degree);
    while !pool.is_empty() && out.len() < degree {
        let picked = pool[0];
        out.push(picked.id);
        pool.retain(|p| p.id != picked.id && alpha * distance(picked.id, p.id) > p.distance);
    }
    out
}
pub(super) const DEGREE: usize = 32;
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
/// Construction and publication peaks are disjoint from retained query scratch.
/// Include worst-case search/prune vectors, including geometrically grown capacities.
pub(super) fn reservation(rows: u64, padded: u64, queries: u64) -> Option<u64> {
    let lock = std::mem::size_of::<RwLock<Neighbors>>() as u64;
    // Codes/scales, row coordinates, insertion permutation, and two construction scratch sets.
    // Each scratch set permits byte marks, expanded IDs, candidate IDs, and scored prune pools.
    let building = padded.checked_add(lock)?.checked_add(16 + 2 * 40)?;
    // Old construction adjacency and the final compact representation coexist during conversion.
    let publication = padded.checked_add(lock)?.checked_add(4 + 4 + 4 + 136 + 1)?;
    let retained = padded.checked_add(4 + 4 + 4 + 136 + 1)?;
    let construction = rows
        .checked_mul(building.max(publication))?
        .checked_add(64 << 20)?;
    let serving = rows.checked_mul(retained)?.checked_add(
        queries.checked_mul(rows.div_ceil(32).checked_mul(8)?.checked_add(2 << 20)?)?,
    )?;
    construction.max(serving).checked_mul(5).map(|n| n / 4)
}
pub fn build(
    v: &Codes,
    entry: u32,
    cancelled: &(impl Fn() -> bool + Sync),
) -> crate::Result<Vec<RwLock<Neighbors>>> {
    let n = v.scales.len();
    let degree = DEGREE.min(n.saturating_sub(1));
    let beam = 64;
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
            || Scratch::new(n),
            |s, &point| {
                if cancelled() {
                    return;
                }
                let mut candidates = s.search(v, &links, entry, point, beam);
                let neighbors = {
                    let mut current = links[point as usize].write();
                    candidates.extend(current.as_slice().iter().copied());
                    let neighbors = prune(point, candidates, degree, alpha, |a, b| v.between(a, b));
                    current.replace(&neighbors);
                    neighbors
                };
                for neighbor in neighbors {
                    let mut incoming = links[neighbor as usize].write();
                    if incoming.as_slice().contains(&point) {
                        continue;
                    }
                    if incoming.as_slice().len() < degree * 2 {
                        incoming.push(point);
                    } else {
                        let mut pool = incoming.as_slice().to_vec();
                        pool.push(point);
                        let pruned = prune(neighbor, pool, degree, alpha, |a, b| v.between(a, b));
                        incoming.replace(&pruned);
                    }
                }
            },
        );
        super::check(cancelled)?;
        links.par_iter().enumerate().for_each(|(point, row)| {
            if cancelled() {
                return;
            }
            let mut row = row.write();
            if row.as_slice().len() > degree {
                let pruned = prune(
                    point as u32,
                    row.as_slice().to_vec(),
                    degree,
                    alpha,
                    |a, b| v.between(a, b),
                );
                row.replace(&pruned);
            }
        });
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
        let result = pool.install(|| {
            build(&codes, 0, &|| {
                checks.fetch_add(1, Ordering::Relaxed) >= 2000
            })
        });
        assert!(
            matches!(result, Err(crate::Error::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted)
        );
        assert!(checks.load(Ordering::Relaxed) >= 2000);
        let nodes = pool.install(|| build(&codes, 0, &|| false)).unwrap();
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
