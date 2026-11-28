//! Cache-blocked full-dimensional scoring, with exact verification against the existing kernel.
use super::{LeafGroups, math};
use std::collections::BTreeMap;

pub(super) struct Block {
    pub rows: Vec<Vec<f32>>,
    pub norms: Vec<f32>,
    pub wanted: Vec<usize>,
}

/// A group is packed once per assignment pass, then reused for every batch that visits it.
enum Matrix {
    Full(math::Centers),
    Reduced(bf_vector::Bf16DotCenters),
}

pub(super) struct Scorer<'a> {
    leaves: &'a [Vec<f32>],
    norms: &'a [f32],
    groups: BTreeMap<usize, (Vec<u32>, Matrix)>,
    reduced: bool,
    matrix_evals: std::sync::atomic::AtomicU64,
    exact_evals: std::sync::atomic::AtomicU64,
}

impl<'a> Scorer<'a> {
    #[cfg(test)]
    pub fn new(leaves: &'a [Vec<f32>], norms: &'a [f32], groups: &[LeafGroups]) -> Self {
        Self::with_bf16(leaves, norms, groups, false)
    }

    pub fn with_bf16(
        leaves: &'a [Vec<f32>],
        norms: &'a [f32],
        groups: &[LeafGroups],
        reduced: bool,
    ) -> Self {
        let groups = groups
            .iter()
            .enumerate()
            .flat_map(|(cell, (_, groups))| {
                groups.iter().enumerate().map(move |(g, ids)| {
                    let packed = if reduced {
                        let refs: Vec<_> =
                            ids.iter().map(|l| leaves[*l as usize].as_slice()).collect();
                        Matrix::Reduced(bf_vector::Bf16DotCenters::new(&refs))
                    } else {
                        Matrix::Full(math::Centers::with_norms(
                            ids.iter()
                                .map(|l| (leaves[*l as usize].as_slice(), norms[*l as usize])),
                        ))
                    };
                    (cell << 32 | g, (ids.clone(), packed))
                })
            })
            .collect();
        Self {
            leaves,
            norms,
            groups,
            reduced,
            matrix_evals: 0.into(),
            exact_evals: 0.into(),
        }
    }

    #[cfg(test)]
    pub fn counts(&self) -> (u64, u64) {
        (
            self.matrix_evals.load(std::sync::atomic::Ordering::Relaxed),
            self.exact_evals.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub fn nearest(&self, blocks: &[Block]) -> Vec<u32> {
        // Keep the original four-row blocks and their candidate unions intact. Only schedule
        // their matrix work together, so the same center block serves many nearby rows.
        let rounded = self.reduced.then(|| {
            let refs: Vec<_> = blocks
                .iter()
                .flat_map(|b| b.rows.iter().map(Vec::as_slice))
                .collect();
            bf_vector::Bf16DotRows::new(&refs)
        });
        let mut visits: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (b, block) in blocks.iter().enumerate() {
            for key in &block.wanted {
                visits.entry(*key).or_default().push(b);
            }
        }
        let mut scores = vec![Vec::<(f32, u32)>::new(); blocks.len() * 4];
        for (key, visiting) in visits {
            let (ids, packed) = &self.groups[&key];
            let norms: Vec<_> = visiting
                .iter()
                .flat_map(|b| blocks[*b].norms.iter().copied())
                .collect();
            let distances = match packed {
                Matrix::Full(packed) => {
                    let rows: Vec<_> = visiting
                        .iter()
                        .flat_map(|b| blocks[*b].rows.iter().map(Vec::as_slice))
                        .collect();
                    packed.distances(&rows, &norms)
                }
                Matrix::Reduced(packed) => {
                    let selected: Vec<_> = visiting
                        .iter()
                        .flat_map(|b| (0..blocks[*b].rows.len()).map(move |r| b * 4 + r))
                        .collect();
                    let mut dots = packed.dots(rounded.as_ref().unwrap(), &selected);
                    for (row, norm) in dots.chunks_exact_mut(ids.len()).zip(&norms) {
                        for (dot, l) in row.iter_mut().zip(ids) {
                            *dot = *norm + self.norms[*l as usize] - 2. * *dot;
                        }
                    }
                    dots
                }
            };
            self.matrix_evals.fetch_add(
                (norms.len() * ids.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let mut at = 0;
            for b in visiting {
                for r in 0..blocks[b].rows.len() {
                    scores[b * 4 + r].extend(
                        distances[at..at + ids.len()]
                            .iter()
                            .copied()
                            .zip(ids.iter().copied()),
                    );
                    at += ids.len();
                }
            }
        }
        let mut out = Vec::with_capacity(blocks.len() * 4);
        for (b, block) in blocks.iter().enumerate() {
            let refs: Vec<_> = block.rows.iter().map(Vec::as_slice).collect();
            let dims = refs[0].len();
            let ne = (dims as f64 + 32.) * f64::from(f32::EPSILON);
            // Both dot-product implementations accumulate at most `dims` products. Their
            // roundoff is bounded by gamma(dims) times sum(abs(products)), itself bounded by
            // the product of L2 norms. The extra factor covers both kernels, rounded norms,
            // distance arithmetic, and gradual underflow. Unsafe scales use the reference path.
            let gamma = ne / (1. - ne);
            let max_norm = block
                .wanted
                .iter()
                .flat_map(|key| self.groups[key].0.iter())
                .map(|l| self.norms[*l as usize])
                .fold(0f32, f32::max);
            let (max_center_norm, max_center_error) =
                block
                    .wanted
                    .iter()
                    .fold((0f64, 0f64), |(norm, error), key| {
                        match &self.groups[key].1 {
                            Matrix::Full(_) => (norm, error),
                            Matrix::Reduced(p) => {
                                (norm.max(p.max_norm_upper()), error.max(p.max_error_upper()))
                            }
                        }
                    });
            let mut thresholds = [f64::INFINITY; 4];
            for r in 0..refs.len() {
                let row = &scores[b * 4 + r];
                if ne < 0.01
                    && block.norms[r].is_finite()
                    && max_norm.is_finite()
                    && block.norms[r] >= 0.
                    && max_norm >= 0.
                    // Leave headroom for both dot accumulators and the final distance sum.
                    && block.norms[r] <= f32::MAX / 16.
                    && max_norm <= f32::MAX / 16.
                    && row.iter().all(|(d, _)| d.is_finite())
                {
                    let minimum = row.iter().map(|(d, _)| *d).fold(f32::INFINITY, f32::min);
                    let mut error = 4. * gamma * (f64::from(block.norms[r]) + f64::from(max_norm))
                        + (dims as f64 + 32.) * f64::from(f32::MIN_POSITIVE);
                    if let Some(rounded) = &rounded {
                        let row = b * 4 + r;
                        // Cauchy-Schwarz bounds the change in a dot product by
                        // ||delta_row|| ||center|| + ||row|| ||delta_center||
                        // + ||delta_row|| ||delta_center||. Squared distances use twice it.
                        error += 2.
                            * (rounded.error_upper(row) * max_center_norm
                                + rounded.norm_upper(row) * max_center_error
                                + rounded.error_upper(row) * max_center_error);
                    }
                    if error.is_finite() {
                        thresholds[r] = f64::from(minimum) + 2. * error;
                    }
                }
            }
            let mut best = [(f32::INFINITY, u32::MAX); 4];
            let mut candidates: Vec<u32> = (0..refs.len())
                .flat_map(|r| {
                    scores[b * 4 + r]
                        .iter()
                        .filter(move |(d, _)| f64::from(*d) <= thresholds[r] || !d.is_finite())
                        .map(|(_, l)| *l)
                })
                .collect();
            candidates.sort_unstable();
            candidates.dedup();
            self.exact_evals.fetch_add(
                (refs.len() * candidates.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            for l in candidates {
                let pick = |j: usize| refs[j.min(refs.len() - 1)];
                let dots = bf_vector::dots_x4(
                    [pick(0), pick(1), pick(2), pick(3)],
                    &self.leaves[l as usize],
                );
                for r in 0..refs.len() {
                    // Re-evaluate the original expression with the original dot kernel and
                    // original row lanes. Ties still choose the lower global leaf number.
                    let d = block.norms[r] + self.norms[l as usize] - 2. * dots[r];
                    if d < best[r].0 || (d == best[r].0 && l < best[r].1) {
                        best[r] = (d, l);
                    }
                }
            }
            out.extend(best[..refs.len()].iter().map(|(_, l)| *l));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::super::norm2;
    use super::*;

    #[test]
    fn screened_winners_match_four_row_reference_including_ties_and_extremes() {
        for dims in [1, 7, 31, 128, 1024] {
            for scale in [0., 1e-20, 1., 1e18, 1e20] {
                let mut leaves: Vec<Vec<f32>> = (0..39)
                    .map(|i| {
                        (0..dims)
                            .map(|d| (((i * 7919 + d * 104729) as f64).sin() as f32) * scale)
                            .collect()
                    })
                    .collect();
                leaves[1] = leaves[0].clone();
                let norms: Vec<_> = leaves.iter().map(|v| norm2(v)).collect();
                let groups = vec![(
                    Vec::new(),
                    vec![(0..13).collect(), (13..26).collect(), (26..39).collect()],
                )];
                let scorer = Scorer::new(&leaves, &norms, &groups);
                let blocks: Vec<_> = (0..7)
                    .map(|b| {
                        let rows: Vec<_> = (0..if b == 6 { 3 } else { 4 })
                            .map(|r| leaves[(b * 4 + r) % leaves.len()].clone())
                            .collect();
                        let norms = rows.iter().map(|v| norm2(v)).collect();
                        Block {
                            rows,
                            norms,
                            wanted: if b % 2 == 0 { vec![0, 2] } else { vec![0, 1] },
                        }
                    })
                    .collect();
                let expected: Vec<_> = blocks
                    .iter()
                    .flat_map(|block| {
                        let refs: Vec<_> = block.rows.iter().map(Vec::as_slice).collect();
                        let ids: Vec<_> = block
                            .wanted
                            .iter()
                            .flat_map(|g| groups[0].1[*g].iter().copied())
                            .collect();
                        let ds =
                            super::super::block_distances(&refs, &block.norms, ids.len(), |n| {
                                (leaves[ids[n] as usize].as_slice(), norms[ids[n] as usize])
                            });
                        ds.into_iter().map(move |row| {
                            let mut best = (f32::INFINITY, u32::MAX);
                            for (d, l) in row.into_iter().zip(&ids) {
                                if d < best.0 || (d == best.0 && *l < best.1) {
                                    best = (d, *l);
                                }
                            }
                            best.1
                        })
                    })
                    .collect();
                assert_eq!(
                    scorer.nearest(&blocks),
                    expected,
                    "dims={dims} scale={scale}"
                );
                let reduced = Scorer::with_bf16(&leaves, &norms, &groups, true);
                assert_eq!(
                    reduced.nearest(&blocks),
                    expected,
                    "BF16 dims={dims} scale={scale}"
                );
            }
        }
    }
    #[test]
    fn independent_and_nearly_tied_rows_keep_exact_winners() {
        let dims = 257;
        let leaves: Vec<Vec<f32>> = (0..127)
            .map(|i| {
                (0..dims)
                    .map(|d| {
                        let base = ((d * 7919) as f64).sin() as f32;
                        base + ((i * 104729 + d * 3571) as f64).cos() as f32
                            * if i < 32 { 1e-5 } else { 0.2 }
                    })
                    .collect()
            })
            .collect();
        let norms: Vec<_> = leaves.iter().map(|v| norm2(v)).collect();
        let groups = vec![(
            Vec::new(),
            vec![
                (0..32).collect(),
                (32..64).collect(),
                (64..96).collect(),
                (96..127).collect(),
            ],
        )];
        let scorer = Scorer::new(&leaves, &norms, &groups);
        for seed in 0..8 {
            let blocks: Vec<_> = (0..32)
                .map(|b| {
                    let rows: Vec<Vec<f32>> = (0..4)
                        .map(|r| {
                            (0..dims)
                                .map(|d| {
                                    ((d * 7919) as f64).sin() as f32
                                        + ((seed * 65537 + b * 7919 + r * 104729 + d * 3571) as f64)
                                            .cos() as f32
                                            * 0.02
                                })
                                .collect()
                        })
                        .collect();
                    let norms = rows.iter().map(|v| norm2(v)).collect();
                    Block {
                        rows,
                        norms,
                        wanted: vec![0, 1, 2, 3],
                    }
                })
                .collect();
            let got = scorer.nearest(&blocks);
            let mut expected = Vec::new();
            for block in &blocks {
                let refs: Vec<_> = block.rows.iter().map(Vec::as_slice).collect();
                let ds = super::super::block_distances(&refs, &block.norms, leaves.len(), |n| {
                    (&leaves[n], norms[n])
                });
                for row in ds {
                    expected.push(
                        row.iter()
                            .enumerate()
                            .min_by(|(a, x), (b, y)| x.total_cmp(y).then(a.cmp(b)))
                            .unwrap()
                            .0 as u32,
                    );
                }
            }
            assert_eq!(got, expected, "seed={seed}");
            let reduced = Scorer::with_bf16(&leaves, &norms, &groups, true);
            assert_eq!(reduced.nearest(&blocks), expected, "BF16 seed={seed}");
        }
    }
    #[test]
    fn near_overflow_distance_ranking_and_ties_match_the_reference() {
        let dims = 128;
        let scale = (f64::from(f32::MAX) / (4. * dims as f64)).sqrt() as f32;
        let leaves: Vec<Vec<f32>> = (0..17)
            .map(|c| {
                (0..dims)
                    .map(|d| {
                        let x = if (c + d) % 3 == 0 {
                            scale.next_down()
                        } else {
                            scale
                        };
                        -x
                    })
                    .collect()
            })
            .collect();
        let norms: Vec<_> = leaves.iter().map(|v| norm2(v)).collect();
        let groups = vec![(Vec::new(), vec![(0..9).collect(), (9..17).collect()])];
        let rows: Vec<Vec<f32>> = (0..4)
            .map(|r| {
                (0..dims)
                    .map(|d| {
                        if (r + d) % 5 == 0 {
                            scale.next_down()
                        } else {
                            scale
                        }
                    })
                    .collect()
            })
            .collect();
        let row_norms = rows.iter().map(|r| norm2(r)).collect();
        let blocks = vec![Block {
            rows,
            norms: row_norms,
            wanted: vec![0, 1],
        }];
        let refs: Vec<_> = blocks[0].rows.iter().map(Vec::as_slice).collect();
        let distances = super::super::block_distances(&refs, &blocks[0].norms, leaves.len(), |c| {
            (&leaves[c], norms[c])
        });
        let expected: Vec<_> = distances
            .into_iter()
            .map(|row| {
                let mut best = (f32::INFINITY, u32::MAX);
                for (c, d) in row.into_iter().enumerate() {
                    if d < best.0 || (d == best.0 && (c as u32) < best.1) {
                        best = (d, c as u32);
                    }
                }
                best.1
            })
            .collect();
        for reduced in [false, true] {
            assert_eq!(
                Scorer::with_bf16(&leaves, &norms, &groups, reduced).nearest(&blocks),
                expected
            );
        }
    }
}
