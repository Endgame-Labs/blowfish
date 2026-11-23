//! Frozen cross-cell trainer and blocked assignment from `src/recluster.rs` at 13ffada.
//! I keep this study copy so the example can inspect centers without changing production APIs.
use rayon::prelude::*;

pub(super) struct Layout {
    pub leaves: Vec<Vec<f32>>,
    pub leaf_of: Vec<u32>,
    pub ranges: Vec<std::ops::Range<usize>>,
}

/// A cell's leaf groups: their centers, and each group's leaves (global leaf numbers).
type LeafGroups = (Vec<Vec<f32>>, Vec<Vec<u32>>);

/// Rows compared with their candidate centers together (see `assign_leaves`).
const ASSIGN_BLOCK: usize = 4;

/// Squared norm of `v`.
fn norm2(v: &[f32]) -> f32 {
    bf_vector::dots_x4([v, v, v, v], v)[0]
}

/// Squared distances from each of `rows` (with squared norms `norms`) to `count` centers given
/// by `center(n)` as (center, squared norm): `out[row][n]`. Four rows share each pass over a
/// center.
#[allow(clippy::needless_range_loop)] // `n` indexes both the centers and each row's output.
fn block_distances<'a>(
    rows: &[&[f32]],
    norms: &[f32],
    count: usize,
    center: impl Fn(usize) -> (&'a [f32], f32),
) -> Vec<Vec<f32>> {
    let mut out = vec![vec![0f32; count]; rows.len()];
    for n in 0..count {
        let (c, cn) = center(n);
        for start in (0..rows.len()).step_by(4) {
            let pick = |j: usize| rows[(start + j).min(rows.len() - 1)];
            let dots = bf_vector::dots_x4([pick(0), pick(1), pick(2), pick(3)], c);
            for (j, dot) in dots.iter().enumerate().take(rows.len() - start) {
                out[start + j][n] = norms[start + j] + cn - 2.0 * dot;
            }
        }
    }
    out
}

/// Coarse cells a row's nearest leaf is looked for in.
const NEAR_CELLS: usize = 4;
/// Training rows per leaf cluster (as the index builder samples).
const LEAF_SAMPLES: usize = 32;
/// Most leaves in a cell that train one-level (about the index builder's two-level threshold).
const ONE_LEVEL_LEAVES: usize = 1024;
/// Leaf groups searched per row, over its near cells.
const GROUP_PROBES: usize = 12;

/// Train each cell's leaf clusters on a sample of its rows, then give every row the nearest leaf
/// among the leaves of its `NEAR_CELLS` nearest cells, and recompute each leaf's center from all
/// of its rows and assign again once. A row near a cell boundary then sits in the cluster
/// whose center is nearest to it, wherever that cluster's slab is, so the clusters a query
/// probes hold more of its neighbors: on BEIR NQ, a row's own cluster was its nearest one only
/// 80% of the time with each cell clustered on its own. I return the centers and row assignments
/// before production slab slicing.
pub(super) fn build(
    present: &[u32],
    read_row: &(dyn Fn(u32, &mut Vec<u8>) -> Vec<f32> + Sync),
    coarse: &[Vec<f32>],
    cell_rows: Vec<Vec<u32>>,
    cluster_rows: usize,
) -> Layout {
    let distance = bf_vector::squared_distance;
    let t0 = std::time::Instant::now();
    let mut leaves: Vec<Vec<f32>> = Vec::new();
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::with_capacity(cell_rows.len());
    // Each cell's leaf groups (global leaf numbers).
    let mut members: Vec<Vec<Vec<u32>>> = Vec::with_capacity(cell_rows.len());
    for (cell, rows) in cell_rows.iter().enumerate() {
        let start = leaves.len();
        if !rows.is_empty() {
            let k = (rows.len() / cluster_rows).max(1);
            // A deterministic sample: a partial Fisher-Yates shuffle seeded by the cell.
            let mut picks = rows.clone();
            let n = (k * LEAF_SAMPLES).min(picks.len());
            let mut state =
                0x2545_F491_4F6C_DD1Du64 ^ (cell as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for i in 0..n {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let j = i + (state as usize) % (picks.len() - i);
                picks.swap(i, j);
            }
            picks.truncate(n);
            let sample: Vec<Vec<f32>> = picks
                .par_iter()
                .map_init(Vec::new, |buf, g| read_row(*g, buf))
                .collect();
            let refs: Vec<&[f32]> = sample.iter().map(|v| v.as_slice()).collect();
            // Leaves are grouped under about sqrt(k) centers for the assignment below. A large
            // cell trains two-level, and its parents' leaves are the groups: one-level training
            // compares every sample with every leaf, so its cost grows with the square of the
            // leaves (5.5 times the per-cell build at 1,700 leaves a cell). A small cell trains
            // one-level (0.003 more recall@10 on NQ at 420 leaves a cell), and its leaves are
            // grouped by a k-means over them.
            let mut cell_groups = Vec::new();
            if k > ONE_LEVEL_LEAVES {
                let (_, children) = bf_vector::kmeans_two_level(&refs, k, 8, 42 + cell as u64);
                for group in children {
                    let first = leaves.len() as u32;
                    leaves.extend(group);
                    cell_groups.push((first..leaves.len() as u32).collect::<Vec<u32>>());
                }
            } else {
                let trained = bf_vector::kmeans(&refs, k, 8, 42 + cell as u64);
                let trained_refs: Vec<&[f32]> = trained.iter().map(Vec::as_slice).collect();
                let g = (trained.len() as f64).sqrt().ceil() as usize;
                let centers = bf_vector::kmeans(&trained_refs, g, 8, 7 + cell as u64);
                cell_groups = vec![Vec::new(); centers.len()];
                for leaf in &trained {
                    cell_groups[bf_vector::nearest_center(leaf, &centers)]
                        .push(leaves.len() as u32);
                    leaves.push(leaf.clone());
                }
                cell_groups.retain(|g| !g.is_empty());
            }
            members.push(cell_groups);
        } else {
            members.push(Vec::new());
        }
        ranges.push(start..leaves.len());
    }
    drop(cell_rows);
    eprintln!(
        "recluster: {} leaves trained in {:.0}s",
        leaves.len(),
        t0.elapsed().as_secs_f64()
    );
    let near = NEAR_CELLS.min(coarse.len()).max(1);
    let probes = GROUP_PROBES;
    let by_distance = |a: &(f32, usize), b: &(f32, usize)| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
    let assign = |leaves: &[Vec<f32>]| -> Vec<u32> {
        let assign_start = std::time::Instant::now();
        // Each cell's leaves under about sqrt(leaves) group centers (the means of their leaves),
        // so a row compares against the groups of its near cells and the leaves of its nearest
        // groups only, not every leaf of every near cell (a 100M layout has about 15,600 leaves a
        // cell).
        let dims = leaves.first().map_or(0, Vec::len);
        let groups: Vec<LeafGroups> = members
            .par_iter()
            .map(|cell_groups| {
                let centers = cell_groups
                    .iter()
                    .map(|group| {
                        let mut sum = vec![0f64; dims];
                        for &l in group {
                            for (s, x) in sum.iter_mut().zip(&leaves[l as usize]) {
                                *s += f64::from(*x);
                            }
                        }
                        sum.iter()
                            .map(|s| (*s / group.len() as f64) as f32)
                            .collect()
                    })
                    .collect();
                (centers, cell_groups.clone())
            })
            .collect();
        // A row's nearest groups among its near cells, nearest first.
        let near_groups = |v: &[f32]| -> Vec<(f32, usize)> {
            let mut cells: Vec<(f32, usize)> = coarse
                .iter()
                .enumerate()
                .map(|(i, c)| (distance(v, c), i))
                .collect();
            if cells.len() > near {
                cells.select_nth_unstable_by(near - 1, by_distance);
                cells.truncate(near);
            }
            let mut found: Vec<(f32, usize)> = Vec::new();
            for (_, cell) in cells {
                for (j, center) in groups[cell].0.iter().enumerate() {
                    found.push((distance(v, center), cell << 32 | j));
                }
            }
            if found.len() > probes {
                found.select_nth_unstable_by(probes - 1, by_distance);
                found.truncate(probes);
            }
            found
        };
        // Leaves are far larger than the CPU caches (13,400 of 4 KB each on BEIR NQ), and rows in
        // ID order are scattered over them, so every comparison would miss. Rows are visited in
        // the order of their nearest group instead: neighbors in that order compare with the same
        // few groups' leaves, which stay cached. The group pass costs a small part of the leaf one.
        let offsets: Vec<usize> = groups
            .iter()
            .scan(0usize, |at, (centers, _)| {
                let start = *at;
                *at += centers.len();
                Some(start)
            })
            .collect();
        let group_count = offsets
            .last()
            .map_or(0, |o| o + groups.last().map_or(0, |g| g.0.len()));
        let nearest_group: Vec<u32> = present
            .par_iter()
            .map_init(Vec::new, |buf, g| {
                let v = read_row(*g, buf);
                near_groups(&v)
                    .into_iter()
                    .min_by(by_distance)
                    .map_or(0, |(_, key)| {
                        (offsets[key >> 32] + (key & 0xFFFF_FFFF)) as u32
                    })
            })
            .collect();
        // A counting sort of row positions by nearest group.
        let mut starts = vec![0usize; group_count + 1];
        for g in &nearest_group {
            starts[*g as usize + 1] += 1;
        }
        for i in 1..starts.len() {
            starts[i] += starts[i - 1];
        }
        let mut order = vec![0u32; present.len()];
        for (i, g) in nearest_group.iter().enumerate() {
            order[starts[*g as usize]] = i as u32;
            starts[*g as usize] += 1;
        }
        drop(nearest_group);
        drop(starts);
        // Rows in blocks of neighbors in that order: a block compares with the union of its rows'
        // nearest groups (nearly the same groups for neighbors), four rows per pass over each
        // center, as dot products against precomputed squared norms.
        let leaf_norms: Vec<f32> = leaves.par_iter().map(|l| norm2(l)).collect();
        let group_norms: Vec<Vec<f32>> = groups
            .iter()
            .map(|(centers, _)| centers.iter().map(|c| norm2(c)).collect())
            .collect();
        let compared = std::sync::atomic::AtomicU64::new(0);
        let ordered: Vec<u32> = order
            .par_chunks(ASSIGN_BLOCK)
            .map_init(Vec::new, |buf, block| {
                let rows: Vec<Vec<f32>> = block
                    .iter()
                    .map(|i| read_row(present[*i as usize], buf))
                    .collect();
                let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
                let norms: Vec<f32> = rows.iter().map(|r| norm2(r)).collect();
                // The block's near cells, and each row's nearest groups among all of their groups.
                let mut cells: Vec<usize> = Vec::new();
                for v in &refs {
                    let mut by_cell: Vec<(f32, usize)> = coarse
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (distance(v, c), i))
                        .collect();
                    if by_cell.len() > near {
                        by_cell.select_nth_unstable_by(near - 1, by_distance);
                        by_cell.truncate(near);
                    }
                    cells.extend(by_cell.into_iter().map(|(_, c)| c));
                }
                cells.sort_unstable();
                cells.dedup();
                let keys: Vec<usize> = cells
                    .iter()
                    .flat_map(|&cell| (0..groups[cell].0.len()).map(move |j| cell << 32 | j))
                    .collect();
                let group_d = block_distances(&refs, &norms, keys.len(), |n| {
                    let key = keys[n];
                    (
                        groups[key >> 32].0[key & 0xFFFF_FFFF].as_slice(),
                        group_norms[key >> 32][key & 0xFFFF_FFFF],
                    )
                });
                let mut wanted: Vec<usize> = Vec::new();
                for d in &group_d {
                    let mut found: Vec<(f32, usize)> =
                        d.iter().enumerate().map(|(n, d)| (*d, keys[n])).collect();
                    if found.len() > probes {
                        found.select_nth_unstable_by(probes - 1, by_distance);
                        found.truncate(probes);
                    }
                    wanted.extend(found.into_iter().map(|(_, key)| key));
                }
                wanted.sort_unstable();
                wanted.dedup();
                let candidates: Vec<u32> = wanted
                    .iter()
                    .flat_map(|key| groups[key >> 32].1[key & 0xFFFF_FFFF].iter().copied())
                    .collect();
                compared.fetch_add(
                    ((keys.len() + candidates.len()) * refs.len()) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                let leaf_d = block_distances(&refs, &norms, candidates.len(), |n| {
                    let l = candidates[n] as usize;
                    (leaves[l].as_slice(), leaf_norms[l])
                });
                leaf_d
                    .iter()
                    .map(|d| {
                        let mut best = (f32::INFINITY, u32::MAX);
                        for (d, &l) in d.iter().zip(&candidates) {
                            if *d < best.0 || (*d == best.0 && l < best.1) {
                                best = (*d, l);
                            }
                        }
                        best.1
                    })
                    .collect::<Vec<u32>>()
            })
            .flatten_iter()
            .collect();
        eprintln!(
            "recluster: {:.0} centers compared per row",
            compared.into_inner() as f64 / present.len().max(1) as f64
        );
        let mut leaf_of = vec![0u32; present.len()];
        for (i, l) in order.iter().zip(ordered) {
            leaf_of[*i as usize] = l;
        }
        println!(
            "BASE_ASSIGN,{},{},{:.6}",
            coarse.len(),
            cluster_rows,
            assign_start.elapsed().as_secs_f64()
        );
        leaf_of
    };
    let mut leaf_of = assign(&leaves);
    eprintln!(
        "recluster: rows assigned at {:.0}s",
        t0.elapsed().as_secs_f64()
    );
    // One refinement: recompute every leaf center from its rows, and assign again. (Stopping at
    // the recomputed centers, without the second assignment, cost 0.003 of recall@10 on NQ.)
    {
        let mut members: Vec<Vec<u32>> = vec![Vec::new(); leaves.len()];
        for (g, l) in present.iter().zip(&leaf_of) {
            members[*l as usize].push(*g);
        }
        let dims = leaves.first().map_or(0, Vec::len);
        let centers: Vec<Vec<f32>> = members
            .par_iter()
            .zip(leaves.par_iter())
            .map_init(Vec::new, |buf, (rows, old)| {
                if rows.is_empty() {
                    return old.clone();
                }
                let mut sum = vec![0f64; dims];
                for g in rows {
                    for (s, x) in sum.iter_mut().zip(read_row(*g, buf)) {
                        *s += f64::from(x);
                    }
                }
                sum.iter()
                    .map(|s| (*s / rows.len() as f64) as f32)
                    .collect()
            })
            .collect();
        leaves = centers;
        leaf_of = assign(&leaves);
        eprintln!("recluster: refined at {:.0}s", t0.elapsed().as_secs_f64());
    }
    Layout {
        leaves,
        leaf_of,
        ranges,
    }
}
