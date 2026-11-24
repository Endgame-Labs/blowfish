//! Experimental centroid graph used by the offline assignment kill test.
use super::{Stage, math};
use rayon::prelude::*;

const DEGREE: usize = 32;
const BEAM: usize = 8;

fn compare(a: &(f32, usize), b: &(f32, usize)) -> std::cmp::Ordering {
    a.0.total_cmp(&b.0).then(a.1.cmp(&b.1))
}

pub(super) struct Graph {
    neighbors: Vec<Vec<usize>>,
    entries: Vec<Vec<usize>>,
    norms: Vec<f32>,
}

impl Graph {
    pub(super) fn build(leaves: &[Vec<f32>], groups: &[Vec<Vec<u32>>]) -> Self {
        let _stage = Stage::new("centroid_graph");
        // Representatives of the existing leaf groups give each row a nearby entry,
        // instead of starting every walk at the cell's one central leaf.
        let entries = groups
            .iter()
            .map(|cell| {
                cell.iter()
                    .map(|group| {
                        let mut mean = vec![0f64; leaves[0].len()];
                        for &leaf in group {
                            for (s, x) in mean.iter_mut().zip(&leaves[leaf as usize]) {
                                *s += f64::from(*x);
                            }
                        }
                        let mean: Vec<_> = mean
                            .iter()
                            .map(|s| (*s / group.len() as f64) as f32)
                            .collect();
                        group
                            .iter()
                            .map(|i| *i as usize)
                            .min_by(|a, b| {
                                bf_vector::squared_distance(&mean, &leaves[*a])
                                    .total_cmp(&bf_vector::squared_distance(&mean, &leaves[*b]))
                                    .then(a.cmp(b))
                            })
                            .unwrap()
                    })
                    .collect()
            })
            .collect();
        // Exact neighbors at NQ scale: the first prototype's cell-wide coarse restriction
        // excluded useful boundary edges. No center is excluded here.
        let packed = math::Centers::new(leaves.iter().map(Vec::as_slice));
        let mut neighbors: Vec<Vec<usize>> = leaves
            .par_chunks(128)
            .enumerate()
            .flat_map_iter(|(chunk, rows)| {
                let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
                let norms: Vec<_> = refs.iter().map(|r| super::norm2(r)).collect();
                packed
                    .distances(&refs, &norms)
                    .chunks(leaves.len())
                    .enumerate()
                    .map(|(local, distances)| {
                        let own = chunk * 128 + local;
                        let mut order: Vec<_> =
                            distances.iter().enumerate().map(|(i, d)| (*d, i)).collect();
                        let keep = (DEGREE + 1).min(order.len());
                        if order.len() > keep {
                            order.select_nth_unstable_by(keep - 1, compare);
                            order.truncate(keep);
                        }
                        order.sort_unstable_by(compare);
                        order
                            .into_iter()
                            .map(|(_, i)| i)
                            .filter(|i| *i != own)
                            .take(DEGREE)
                            .collect()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        // Reverse edges make cell boundaries traversable from either side. Iteration and edge
        // ordering are fixed, so parallel graph construction cannot affect a walk's tie breaks.
        let forward = neighbors.clone();
        for (i, edges) in forward.iter().enumerate() {
            for &j in edges {
                neighbors[j].push(i);
            }
        }
        for edges in &mut neighbors {
            edges.sort_unstable();
            edges.dedup();
        }
        println!(
            "RR3 graph degree mean={:.2} max={}",
            neighbors.iter().map(Vec::len).sum::<usize>() as f64 / leaves.len() as f64,
            neighbors.iter().map(Vec::len).max().unwrap_or(0)
        );
        let norms = leaves.iter().map(|v| super::norm2(v)).collect();
        Self {
            neighbors,
            entries,
            norms,
        }
    }

    fn distances(&self, v: &[f32], norm: f32, leaves: &[Vec<f32>], ids: &[usize]) -> [f32; 4] {
        let pick = |i: usize| ids[i.min(ids.len() - 1)];
        if norm > f32::MAX / 4.
            || !norm.is_finite()
            || ids
                .iter()
                .any(|i| self.norms[*i] > f32::MAX / 4. || !self.norms[*i].is_finite())
        {
            return std::array::from_fn(|i| bf_vector::squared_distance(v, &leaves[pick(i)]));
        }
        let dots = bf_vector::dots_x4(std::array::from_fn(|i| leaves[pick(i)].as_slice()), v);
        std::array::from_fn(|i| norm + self.norms[pick(i)] - 2. * dots[i])
    }

    pub(super) fn walk(
        &self,
        v: &[f32],
        leaves: &[Vec<f32>],
        coarse: &[Vec<f32>],
        visited: &mut [u32],
        epoch: u32,
    ) -> (usize, usize) {
        let cell = coarse
            .iter()
            .enumerate()
            .filter(|(i, _)| !self.entries[*i].is_empty())
            .map(|(i, c)| (bf_vector::squared_distance(v, c), i))
            .min_by(compare)
            .unwrap()
            .1;
        let mut compared = coarse.len() + self.entries[cell].len();
        let norm = super::norm2(v);
        let mut beam = Vec::with_capacity(self.entries[cell].len().max(BEAM + 1));
        for ids in self.entries[cell].chunks(4) {
            let distances = self.distances(v, norm, leaves, ids);
            for (&seed, d) in ids.iter().zip(distances) {
                visited[seed] = epoch;
                beam.push((d, seed, false));
            }
        }
        beam.sort_unstable_by(|a, b| compare(&(a.0, a.1), &(b.0, b.1)));
        beam.truncate(BEAM);
        fn offer(beam: &mut Vec<(f32, usize, bool)>, d: f32, i: usize) {
            if beam.len() < BEAM
                || compare(&(d, i), &(beam.last().unwrap().0, beam.last().unwrap().1)).is_lt()
            {
                let at =
                    beam.partition_point(|entry| compare(&(entry.0, entry.1), &(d, i)).is_lt());
                beam.insert(at, (d, i, false));
                if beam.len() > BEAM {
                    beam.pop();
                }
            }
        }
        while let Some(next) = beam.iter().position(|(_, _, expanded)| !expanded) {
            let (_, leaf, _) = beam[next];
            beam[next].2 = true;
            let mut pending = [0usize; 4];
            let mut count = 0;
            for &i in &self.neighbors[leaf] {
                if visited[i] == epoch {
                    continue;
                }
                visited[i] = epoch;
                compared += 1;
                pending[count] = i;
                count += 1;
                if count == 4 {
                    for (i, d) in pending
                        .into_iter()
                        .zip(self.distances(v, norm, leaves, &pending))
                    {
                        offer(&mut beam, d, i);
                    }
                    count = 0;
                }
            }
            if count > 0 {
                let distances = self.distances(v, norm, leaves, &pending[..count]);
                for (&i, d) in pending[..count].iter().zip(distances) {
                    offer(&mut beam, d, i);
                }
            }
        }
        (beam[0].1, compared)
    }

    pub(super) fn assign(
        &self,
        leaves: &[Vec<f32>],
        coarse: &[Vec<f32>],
        present: &[u32],
        read_row: &(dyn Fn(u32, &mut Vec<u8>) -> Vec<f32> + Sync),
        previous: Option<&[u32]>,
        visit_order: Option<&[usize]>,
    ) -> Vec<u32> {
        let _stage = Stage::new("graph_assignment");
        if let Some(previous) = previous {
            let mut members = vec![Vec::new(); leaves.len()];
            for (i, leaf) in previous.iter().enumerate() {
                members[*leaf as usize].push(i);
            }
            let mut out = previous.to_vec();
            // Rows that move get another dense neighbor pass at their new leaf. Fixed ordering
            // and a bounded number of hops preserve reproducibility and bounded work.
            for _ in 0..4 {
                let groups: Vec<Vec<(usize, u32)>> = members
                    .par_iter()
                    .enumerate()
                    .map(|(leaf, members)| {
                        if members.is_empty() {
                            return Vec::new();
                        }
                        let mut candidates = self.neighbors[leaf].clone();
                        candidates.push(leaf);
                        candidates.sort_unstable();
                        let packed =
                            math::Centers::new(candidates.iter().map(|i| leaves[*i].as_slice()));
                        let mut buf = Vec::new();
                        members
                            .chunks(128)
                            .flat_map(|chunk| {
                                let vectors: Vec<_> = chunk
                                    .iter()
                                    .map(|i| read_row(present[*i], &mut buf))
                                    .collect();
                                let refs: Vec<_> = vectors.iter().map(Vec::as_slice).collect();
                                chunk
                                    .iter()
                                    .zip(packed.nearest(&refs))
                                    .map(|(i, c)| (*i, candidates[c] as u32))
                                    .collect::<Vec<_>>()
                            })
                            .collect()
                    })
                    .collect();
                let mut next = vec![Vec::new(); leaves.len()];
                for (i, leaf) in groups.into_iter().flatten() {
                    if out[i] != leaf {
                        out[i] = leaf;
                        next[leaf as usize].push(i);
                    }
                }
                if next.iter().all(Vec::is_empty) {
                    break;
                }
                for group in &mut next {
                    group.sort_unstable();
                }
                members = next;
            }
            out
        } else {
            let natural: Vec<usize> = (0..present.len()).collect();
            let order = visit_order.unwrap_or(&natural);
            let grouped: Vec<(Vec<(usize, u32)>, usize)> = order
                .par_chunks(2048)
                .map(|chunk| {
                    let mut visited = vec![0; leaves.len()];
                    let mut buf = Vec::new();
                    let mut compared = 0;
                    let found = chunk
                        .iter()
                        .enumerate()
                        .map(|(i, position)| {
                            let (leaf, count) = self.walk(
                                &read_row(present[*position], &mut buf),
                                leaves,
                                coarse,
                                &mut visited,
                                i as u32 + 1,
                            );
                            compared += count;
                            (*position, leaf as u32)
                        })
                        .collect::<Vec<_>>();
                    (found, compared)
                })
                .collect();
            let mut out = vec![0; present.len()];
            println!(
                "RR3 graph centers per row={:.2}",
                grouped.iter().map(|(_, n)| n).sum::<usize>() as f64 / present.len() as f64
            );
            for (i, leaf) in grouped.into_iter().flat_map(|(rows, _)| rows) {
                out[i] = leaf;
            }
            out
        }
    }
}
