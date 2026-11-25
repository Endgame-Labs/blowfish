//! Certified group routing shared by ordering and four-row assignment.
//!
//! Ordering uses direct squared distances, whereas assignment uses the original norm/dot
//! expression. BF16 scores only screen both computations: ordering rechecks its possible
//! minima with direct distances, and assignment rechecks its possible top groups in the
//! original four-row lanes. A group's numerical score is never reused as an exact score.
use super::{LeafGroups, NEAR_CELLS};
use bf_vector::{Bf16DotCenters, Bf16DotRows};

const MAX_CACHED_GROUPS: usize = 32;

/// Keep the worst-case cache and ordering buffers within an eighth of the memory
/// limit, capped at 1 GiB. Larger tiers retain the original uncached routing path.
pub(super) fn fits(rows: usize) -> bool {
    let budget = (bf_alloc::memory_limit_bytes() / 8).min(1 << 30) as usize;
    rows.checked_mul(
        2 * std::mem::size_of::<Row>() + MAX_CACHED_GROUPS * std::mem::size_of::<usize>(),
    )
    .is_some_and(|bytes| bytes <= budget)
}

pub(super) struct Row {
    pub nearest: u32,
    pub cells: [u8; NEAR_CELLS],
    // None retains all of this row's groups. Bound per-row cache growth on ties and unsafe input.
    pub candidates: Option<Vec<usize>>,
    pub cutoff: f64,
    pub norm: f32,
}

pub(super) struct Cached {
    pub candidates: Option<Vec<usize>>,
    pub cutoff: f64,
    pub norm: f32,
}

pub(super) struct Router<'a> {
    groups: &'a [LeafGroups],
    norms: &'a [Vec<f32>],
    packed: Vec<Option<Bf16DotCenters>>,
    offsets: Vec<usize>,
    max_norm: f64,
    max_error: f64,
    safe: bool,
}

fn compare(a: &(f32, usize), b: &(f32, usize)) -> std::cmp::Ordering {
    a.0.total_cmp(&b.0).then(a.1.cmp(&b.1))
}

impl<'a> Router<'a> {
    pub fn new(groups: &'a [LeafGroups], norms: &'a [Vec<f32>]) -> Self {
        let packed: Vec<_> = groups
            .iter()
            .map(|(centers, _)| {
                (!centers.is_empty()).then(|| {
                    let refs: Vec<_> = centers.iter().map(Vec::as_slice).collect();
                    Bf16DotCenters::new(&refs)
                })
            })
            .collect();
        let offsets = groups
            .iter()
            .scan(0, |at, (centers, _)| {
                let start = *at;
                *at += centers.len();
                Some(start)
            })
            .collect();
        Self {
            groups,
            norms,
            max_norm: packed
                .iter()
                .flatten()
                .map(Bf16DotCenters::max_norm_upper)
                .fold(0., f64::max),
            max_error: packed
                .iter()
                .flatten()
                .map(Bf16DotCenters::max_error_upper)
                .fold(0., f64::max),
            safe: norms
                .iter()
                .flatten()
                .all(|n| n.is_finite() && *n >= 0. && *n <= f32::MAX / 16.),
            packed,
            offsets,
        }
    }

    /// Score only each row's original near cells, in bounded matrix batches.
    pub fn order(
        &self,
        rows: &[Vec<f32>],
        coarse: &[Vec<f32>],
        cached: Option<&[[u8; NEAR_CELLS]]>,
        probes: usize,
    ) -> Vec<Row> {
        assert!(coarse.len() <= 256 && !coarse.is_empty() && probes > 0);
        let near = NEAR_CELLS.min(coarse.len());
        let cells: Vec<_> = rows
            .iter()
            .enumerate()
            .map(|(r, row)| {
                if let Some(cached) = cached {
                    return cached[r];
                }
                let mut found: Vec<_> = coarse
                    .iter()
                    .enumerate()
                    .map(|(c, center)| (bf_vector::squared_distance(row, center), c))
                    .collect();
                if found.len() > near {
                    found.select_nth_unstable_by(near - 1, compare);
                    found.truncate(near);
                }
                let mut ids = [0; NEAR_CELLS];
                for (id, (_, c)) in ids.iter_mut().zip(found) {
                    *id = c as u8;
                }
                ids
            })
            .collect();
        let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
        let rounded = Bf16DotRows::new(&refs);
        let norms: Vec<_> = rows.iter().map(|v| super::norm2(v)).collect();
        let mut scores = vec![Vec::<(f32, usize)>::new(); rows.len()];
        for (cell, packed) in self.packed.iter().enumerate() {
            let Some(packed) = packed else {
                continue;
            };
            let visiting: Vec<_> = cells
                .iter()
                .enumerate()
                .filter(|(_, cs)| cs[..near].contains(&(cell as u8)))
                .map(|(r, _)| r)
                .collect();
            let dots = packed.dots(&rounded, &visiting);
            for (ds, r) in dots.chunks_exact(self.norms[cell].len()).zip(visiting) {
                scores[r].extend(
                    ds.iter().enumerate().map(|(g, dot)| {
                        (norms[r] + self.norms[cell][g] - 2. * dot, cell << 32 | g)
                    }),
                );
            }
        }
        scores
            .into_iter()
            .enumerate()
            .map(|(r, mut scores)| {
                let dims = rows[r].len();
                let ne = (dims as f64 + 32.) * f64::from(f32::EPSILON);
                let rn = rounded.norm_upper(r);
                // Cover conversion with Cauchy-Schwarz, and roundoff in both distance
                // formulations with gamma(dims + 32). Direct L2's subtraction, product,
                // and positive sum, the dot kernels, rounded norms, and final subtraction
                // use fewer than 16 gamma times the original squared-norm sum. Bounds
                // use f64 norm upper bounds, and leave FP32 overflow headroom.
                let error = if self.safe
                    && ne < 0.01
                    && rn.is_finite()
                    && rn * rn <= f64::from(f32::MAX / 16.)
                    && self.max_norm * self.max_norm <= f64::from(f32::MAX / 16.)
                    && norms[r].is_finite()
                    && norms[r] >= 0.
                    && scores.iter().all(|(d, _)| d.is_finite())
                {
                    let er = rounded.error_upper(r);
                    (16. * ne / (1. - ne) * (rn * rn + self.max_norm * self.max_norm)
                        + 2. * (er * self.max_norm + rn * self.max_error + er * self.max_error)
                        + 16. * (dims as f64 + 32.) * f64::from(f32::MIN_POSITIVE))
                    .next_up()
                } else {
                    f64::INFINITY
                };
                let minimum = scores.iter().map(|(d, _)| *d).fold(f32::INFINITY, f32::min);
                let threshold = if error.is_finite() {
                    f64::from(minimum) + 2. * error
                } else {
                    f64::INFINITY
                };
                let nearest = scores
                    .iter()
                    .filter(|(d, _)| !d.is_finite() || f64::from(*d) <= threshold)
                    .map(|(_, key)| {
                        (
                            bf_vector::squared_distance(
                                &rows[r],
                                &self.groups[key >> 32].0[key & 0xffff_ffff],
                            ),
                            *key,
                        )
                    })
                    .min_by(compare)
                    .map_or(0, |(_, key)| {
                        (self.offsets[key >> 32] + (key & 0xffff_ffff)) as u32
                    });
                let mut cutoff = f64::INFINITY;
                let candidates = if error.is_finite() && scores.len() > probes {
                    scores.select_nth_unstable_by(probes - 1, compare);
                    let threshold = f64::from(scores[probes - 1].0) + 2. * error;
                    cutoff = threshold;
                    // At least `probes` original own-cell distances are <= kth_approx + error.
                    // Anything above kth_approx + 2*error cannot join those top groups,
                    // even after the four-row block adds the neighboring rows' cells.
                    let mut keys: Vec<_> = scores
                        .iter()
                        .filter(|(d, _)| f64::from(*d) <= threshold)
                        .map(|(_, key)| *key)
                        .collect();
                    if keys.len() <= MAX_CACHED_GROUPS && keys.capacity() <= MAX_CACHED_GROUPS {
                        keys.sort_unstable();
                        Some(keys)
                    } else {
                        None
                    }
                } else {
                    None
                };
                Row {
                    nearest,
                    cells: cells[r],
                    candidates,
                    cutoff,
                    norm: norms[r],
                }
            })
            .collect()
    }
    /// Screen cells supplied by neighboring rows in matrix batches as well. Own-cell
    /// cutoffs are upper bounds on the block's original kth group distance. Both the
    /// old own-cell matrix and this missing-cell matrix have the same certified error,
    /// so the stored kth + twice-error cutoff applies to either one. Original four-row
    /// dot products decide every surviving group's actual rank.
    pub fn assign(
        &self,
        blocks: &mut [super::assignment::Block],
        positions: &[u32],
        cache: &[Cached],
        cells: &[[u8; NEAR_CELLS]],
        probes: usize,
    ) -> Bf16DotRows {
        let refs: Vec<_> = blocks
            .iter()
            .flat_map(|b| b.rows.iter().map(Vec::as_slice))
            .collect();
        let rounded = Bf16DotRows::new(&refs);
        let near = NEAR_CELLS.min(self.groups.len());
        let cached: Vec<_> = positions.iter().map(|p| &cache[*p as usize]).collect();
        let own: Vec<_> = positions.iter().map(|p| cells[*p as usize]).collect();
        let norms: Vec<_> = blocks
            .iter()
            .flat_map(|b| b.norms.iter().copied())
            .collect();
        let mut masks = vec![[0u64; 4]; blocks.len()];
        let mut allowed = vec![Vec::new(); refs.len()];
        for (b, block) in blocks.iter().enumerate() {
            for key in &block.wanted {
                let cell = key >> 32;
                masks[b][cell / 64] |= 1 << (cell % 64);
                for r in 0..block.rows.len() {
                    let at = b * 4 + r;
                    if if own[at][..near].contains(&(cell as u8)) {
                        cached[at]
                            .candidates
                            .as_ref()
                            .is_none_or(|keys| keys.binary_search(key).is_ok())
                    } else {
                        !cached[at].cutoff.is_finite()
                    } {
                        allowed[at].push(*key);
                    }
                }
            }
        }
        for (cell, packed) in self.packed.iter().enumerate() {
            let Some(packed) = packed else {
                continue;
            };
            let visiting: Vec<_> = (0..refs.len())
                .filter(|r| {
                    masks[*r / 4][cell / 64] & (1 << (cell % 64)) != 0
                        && !own[*r][..near].contains(&(cell as u8))
                        && cached[*r].cutoff.is_finite()
                })
                .collect();
            let dots = packed.dots(&rounded, &visiting);
            for (ds, r) in dots.chunks_exact(self.norms[cell].len()).zip(visiting) {
                for (g, dot) in ds.iter().enumerate() {
                    let distance = norms[r] + self.norms[cell][g] - 2. * dot;
                    if !distance.is_finite() || f64::from(distance) <= cached[r].cutoff {
                        allowed[r].push(cell << 32 | g);
                    }
                }
            }
        }
        // Drop borrowed row views before replacing the blocks' candidate lists.
        drop(refs);
        for (b, block) in blocks.iter_mut().enumerate() {
            let mut keys: Vec<_> = allowed[b * 4..b * 4 + block.rows.len()]
                .iter()
                .flatten()
                .copied()
                .collect();
            keys.sort_unstable();
            keys.dedup();
            let refs: Vec<_> = block.rows.iter().map(Vec::as_slice).collect();
            let distances = super::block_distances(&refs, &block.norms, keys.len(), |n| {
                let k = keys[n];
                (
                    self.groups[k >> 32].0[k & 0xffff_ffff].as_slice(),
                    self.norms[k >> 32][k & 0xffff_ffff],
                )
            });
            let mut wanted = Vec::new();
            for distances in distances {
                let mut found: Vec<_> = distances.into_iter().zip(keys.iter().copied()).collect();
                if found.len() > probes {
                    found.select_nth_unstable_by(probes - 1, compare);
                    found.truncate(probes);
                }
                wanted.extend(found.into_iter().map(|(_, key)| key));
            }
            wanted.sort_unstable();
            wanted.dedup();
            block.wanted = wanted;
        }
        rounded
    }
}

/// A cached row may exclude only groups from its own near cells. Every group in a cell
/// supplied by a neighbor must still be scored for that row, preserving the original union.
#[cfg(test)]
pub(super) fn needed(key: usize, cells: &[u8], candidates: &Option<Vec<usize>>) -> bool {
    !cells.iter().any(|c| usize::from(*c) == key >> 32)
        || candidates
            .as_ref()
            .is_none_or(|keys| keys.binary_search(&key).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn neighboring_cells_and_refused_caches_are_never_excluded() {
        let selected = Some(vec![1, 4, 2usize << 32]);
        assert!(needed(1, &[0, 2], &selected));
        assert!(!needed(3, &[0, 2], &selected));
        assert!(needed(1usize << 32 | 3, &[0, 2], &selected));
        assert!(needed(3, &[0, 2], &None));
    }

    #[test]
    fn ordering_and_pooled_top_groups_match_both_original_kernels() {
        for dims in [1, 7, 128, 1024] {
            for scale in [0., 1e-20, 1., 1e18, 1e20] {
                let coarse: Vec<_> = (0..6)
                    .map(|c| {
                        (0..dims)
                            .map(|d| ((c * 7919 + d * 104729) as f64).sin() as f32 * scale)
                            .collect::<Vec<_>>()
                    })
                    .collect();
                let groups: Vec<LeafGroups> = (0..6)
                    .map(|c| {
                        let centers = (0..23)
                            .map(|g| {
                                (0..dims)
                                    .map(|d| {
                                        ((c * 997 + g * 7919 + d * 104729) as f64).sin() as f32
                                            * scale
                                    })
                                    .collect()
                            })
                            .collect();
                        (centers, vec![Vec::new(); 23])
                    })
                    .collect();
                let norms: Vec<Vec<_>> = groups
                    .iter()
                    .map(|(cs, _)| cs.iter().map(|c| super::super::norm2(c)).collect())
                    .collect();
                let rows: Vec<_> = (0..11)
                    .map(|r| groups[r % 6].0[(r * 7) % 23].clone())
                    .collect();
                let router = Router::new(&groups, &norms);
                let routed = router.order(&rows, &coarse, None, 12);
                if dims >= 128 && scale == 1. {
                    assert!(routed.iter().any(|r| r.candidates.is_some()));
                }
                let cached: Vec<_> = routed.iter().map(|r| r.cells).collect();
                let rerouted = router.order(&rows, &coarse, Some(&cached), 12);
                let cache: Vec<_> = routed
                    .iter()
                    .map(|r| Cached {
                        candidates: r.candidates.clone(),
                        cutoff: r.cutoff,
                        norm: r.norm,
                    })
                    .collect();
                let mut pooled: Vec<_> = rows
                    .chunks(4)
                    .zip(routed.chunks(4))
                    .map(|(rs, routes)| {
                        let mut cells: Vec<_> = routes.iter().flat_map(|r| r.cells).collect();
                        cells.sort_unstable();
                        cells.dedup();
                        super::super::assignment::Block {
                            positions: Vec::new(),
                            rows: rs.to_vec(),
                            norms: rs.iter().map(|r| super::super::norm2(r)).collect(),
                            wanted: cells
                                .into_iter()
                                .flat_map(|c| (0..23).map(move |g| usize::from(c) << 32 | g))
                                .collect(),
                        }
                    })
                    .collect();
                let positions: Vec<_> = (0..rows.len() as u32).collect();
                router.assign(&mut pooled, &positions, &cache, &cached, 12);
                for (a, b) in routed.iter().zip(&rerouted) {
                    assert_eq!(a.nearest, b.nearest);
                    assert_eq!(a.candidates, b.candidates);
                }
                for (row, route) in rows.iter().zip(&routed) {
                    let mut reference_cells: Vec<_> = coarse
                        .iter()
                        .enumerate()
                        .map(|(c, center)| (bf_vector::squared_distance(row, center), c))
                        .collect();
                    reference_cells.sort_unstable_by(compare);
                    reference_cells.truncate(NEAR_CELLS);
                    let mut reference_cells: Vec<_> =
                        reference_cells.into_iter().map(|(_, c)| c as u8).collect();
                    reference_cells.sort_unstable();
                    let mut actual_cells = route.cells;
                    actual_cells.sort_unstable();
                    assert_eq!(actual_cells.as_slice(), reference_cells);
                    let best = route
                        .cells
                        .iter()
                        .flat_map(|c| {
                            groups[*c as usize]
                                .0
                                .iter()
                                .enumerate()
                                .map(move |(g, center)| {
                                    (
                                        bf_vector::squared_distance(row, center),
                                        usize::from(*c) * 23 + g,
                                    )
                                })
                        })
                        .min_by(compare)
                        .unwrap()
                        .1;
                    assert_eq!(route.nearest as usize, best, "dims={dims} scale={scale}");
                }
                for (b, (block, routes)) in rows.chunks(4).zip(routed.chunks(4)).enumerate() {
                    let mut cells: Vec<_> = routes.iter().flat_map(|r| r.cells).collect();
                    cells.sort_unstable();
                    cells.dedup();
                    let keys: Vec<_> = cells
                        .iter()
                        .flat_map(|c| (0..23).map(move |g| usize::from(*c) << 32 | g))
                        .collect();
                    let kept: Vec<_> = keys
                        .iter()
                        .copied()
                        .filter(|key| routes.iter().any(|r| needed(*key, &r.cells, &r.candidates)))
                        .collect();
                    let refs: Vec<_> = block.iter().map(Vec::as_slice).collect();
                    let ns: Vec<_> = block.iter().map(|r| super::super::norm2(r)).collect();
                    let distances = |keys: &[usize]| {
                        super::super::block_distances(&refs, &ns, keys.len(), |n| {
                            let k = keys[n];
                            (
                                groups[k >> 32].0[k & 0xffff_ffff].as_slice(),
                                norms[k >> 32][k & 0xffff_ffff],
                            )
                        })
                    };
                    let top = |ds: Vec<Vec<f32>>, keys: &[usize]| {
                        ds.into_iter()
                            .map(|ds| {
                                let mut found: Vec<_> =
                                    ds.into_iter().zip(keys.iter().copied()).collect();
                                found.sort_unstable_by(compare);
                                found.truncate(12);
                                found.into_iter().map(|(_, k)| k).collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>()
                    };
                    let expected = top(distances(&keys), &keys);
                    let mut expected_union: Vec<_> = expected.iter().flatten().copied().collect();
                    expected_union.sort_unstable();
                    expected_union.dedup();
                    assert_eq!(
                        pooled[b].wanted, expected_union,
                        "missing-cell screening dims={dims} scale={scale}"
                    );
                    assert_eq!(
                        expected,
                        top(distances(&kept), &kept),
                        "dims={dims} scale={scale}"
                    );
                }
            }
        }
    }
}
