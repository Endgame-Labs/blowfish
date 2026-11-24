//! Compact, disposable Vamana construction. Two passes, with reciprocal robust pruning.
//! Locks protect only adjacency; a search copies one list before releasing its read lock.
use super::data::Vectors;
use rayon::prelude::*;
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    sync::RwLock,
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
        v: &Vectors,
        links: &[RwLock<Vec<u32>>],
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
            let neighbors = links[current.id as usize].read().unwrap().clone();
            for id in neighbors {
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
pub fn build(v: &Vectors, entry: u32, degree: usize, beam: usize) -> Vec<Vec<u32>> {
    let n = v.len();
    let degree = degree.min(n.saturating_sub(1));
    let links = (0..n)
        .map(|i| {
            let mut seed = i as u64 + 7;
            let mut neighbors = Vec::with_capacity(degree);
            while neighbors.len() < degree {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let id = (seed % n as u64) as u32;
                if id != i as u32 && !neighbors.contains(&id) {
                    neighbors.push(id);
                }
            }
            RwLock::new(neighbors)
        })
        .collect::<Vec<_>>();
    let mut ids = (0..n as u32).collect::<Vec<_>>();
    let mut seed = 7u64;
    for i in (1..n).rev() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        ids.swap(i, seed as usize % (i + 1));
    }
    for alpha in [1., 1.2] {
        ids.par_iter().for_each_init(
            || Scratch::new(n),
            |s, &point| {
                let mut candidates = s.search(v, &links, entry, point, beam);
                let neighbors = {
                    // Preserve reciprocal additions that arrived while this point was searched.
                    let mut current = links[point as usize].write().unwrap();
                    candidates.extend(current.iter().copied());
                    let neighbors = prune(point, candidates, degree, alpha, |a, b| v.between(a, b));
                    *current = neighbors.clone();
                    neighbors
                };
                for neighbor in neighbors {
                    let mut incoming = links[neighbor as usize].write().unwrap();
                    if !incoming.contains(&point) {
                        incoming.push(point);
                        if incoming.len() > degree {
                            *incoming = prune(neighbor, incoming.clone(), degree, alpha, |a, b| {
                                v.between(a, b)
                            });
                        }
                    }
                }
            },
        );
    }
    links.into_iter().map(|l| l.into_inner().unwrap()).collect()
}
#[cfg(test)]
mod tests {
    use super::super::data::Matrix;
    use super::*;
    use half::f16;
    #[test]
    fn reciprocal_build_has_valid_bounded_edges_and_finds_self() {
        let bytes = (0..160 * 16)
            .flat_map(|i| f16::from_f32((i as f32 * 0.127).sin()).to_le_bytes())
            .collect();
        let v = Vectors::from_matrix(Matrix {
            rows: 160,
            cols: 16,
            bytes,
        });
        let graph = build(&v, 0, 16, 64);
        assert!(graph.iter().all(|n| n.len() <= 16));
        assert!(
            graph
                .iter()
                .enumerate()
                .all(|(i, n)| n.iter().all(|p| *p < 160 && *p != i as u32))
        );
        let locked = graph.into_iter().map(RwLock::new).collect::<Vec<_>>();
        let mut s = Scratch::new(160);
        for q in [0, 7, 50, 159] {
            assert!(s.search(&v, &locked, 0, q, 160).contains(&q));
        }
    }
    #[test]
    fn robust_prune_removes_self_duplicates_and_redundant_neighbors() {
        let p = prune(0, vec![0, 1, 1, 2, 3], 2, 1.2, |a, b| {
            (a as f32 - b as f32).abs()
        });
        assert_eq!(p, vec![1]);
    }
}
