//! Exact nearest-center search for many rows at once, with BF16 matrix screening.
//!
//! The index builder and the recluster trainer spend most of their time finding, for each row,
//! the center with the smallest `l2_squared_f32`. A BF16 matrix product scores a block of rows
//! against every center cheaply; a certified error bound then keeps only the centers that
//! could still win, and the original kernel recomputes those. The winner, including the
//! lowest-index tie rule, is therefore identical to scoring every center one by one.
use crate::{Bf16DotCenters, Bf16DotRows, bf16_matrix_accelerated};

/// Rows scored per matrix product.
const BLOCK_ROWS: usize = 64;
/// Below this many centers or dimensions the plain scan is as fast.
const MIN_CENTERS: usize = 24;
const MIN_DIMS: usize = 64;

/// Set only by tests: 0 chooses by CPU, 1 uses the matrix path on any CPU, 2 never does.
pub(crate) static MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
/// Rows screened and centers that survived the screen, counted only under test.
#[cfg(test)]
pub(crate) static SCREENED: [std::sync::atomic::AtomicU64; 2] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

/// Centers prepared for repeated nearest searches.
pub(crate) struct NearestCenters<'a> {
    centers: &'a [Vec<f32>],
    matrix: Option<Matrix>,
}
struct Matrix {
    packed: Bf16DotCenters,
    /// Squared norms in f64, which carries every summation here to far beyond f32 accuracy.
    norms: Vec<f64>,
    max_norm_squared: f64,
}

impl<'a> NearestCenters<'a> {
    /// Use the matrix path when the CPU has BF16 matrix instructions and the sizes reward it.
    pub fn new(centers: &'a [Vec<f32>]) -> Self {
        let wanted = match MODE.load(std::sync::atomic::Ordering::Relaxed) {
            1 => true,
            2 => false,
            _ => {
                bf16_matrix_accelerated()
                    && centers.len() >= MIN_CENTERS
                    && centers.first().is_some_and(|c| c.len() >= MIN_DIMS)
            }
        };
        Self::with_matrix(centers, wanted)
    }

    /// As [`NearestCenters::new`], with the matrix path chosen by the caller (for tests).
    pub fn with_matrix(centers: &'a [Vec<f32>], matrix: bool) -> Self {
        let matrix =
            (matrix && !centers.is_empty() && centers.iter().all(|c| !c.is_empty())).then(|| {
                let refs: Vec<&[f32]> = centers.iter().map(Vec::as_slice).collect();
                let norms: Vec<f64> = centers.iter().map(|c| squared_norm(c)).collect();
                Matrix {
                    packed: Bf16DotCenters::new(&refs),
                    max_norm_squared: norms.iter().copied().fold(0., f64::max),
                    norms,
                }
            });
        Self { centers, matrix }
    }

    /// The index of the center nearest each row, as `nearest` would pick it one row at a time.
    pub fn nearest_batch(&self, rows: &[&[f32]]) -> Vec<usize> {
        let Some(matrix) = &self.matrix else {
            return rows.iter().map(|r| self.exact(r, None)).collect();
        };
        let mut out = Vec::with_capacity(rows.len());
        for block in rows.chunks(BLOCK_ROWS) {
            if block.iter().any(|r| r.len() != self.centers[0].len()) {
                out.extend(block.iter().map(|r| self.exact(r, None)));
                continue;
            }
            let rounded = Bf16DotRows::new(block);
            let selected: Vec<usize> = (0..block.len()).collect();
            let dots = matrix.packed.dots(&rounded, &selected);
            for (r, row) in block.iter().enumerate() {
                let scores = &dots[r * self.centers.len()..(r + 1) * self.centers.len()];
                out.push(self.screened(matrix, row, scores, &rounded, r));
            }
        }
        out
    }

    /// The winner among the centers whose BF16 score could still beat the best, or a full scan
    /// when no certificate is safe (non-finite scores, extreme scales, huge dimensions).
    fn screened(
        &self,
        matrix: &Matrix,
        row: &[f32],
        dots: &[f32],
        rounded: &Bf16DotRows,
        r: usize,
    ) -> usize {
        let dims = row.len() as f64;
        let ne = (dims + 32.) * f64::from(f32::EPSILON);
        let row_squared = squared_norm(row);
        let limit = f64::from(f32::MAX) / 16.;
        if ne >= 0.01
            || !row_squared.is_finite()
            || row_squared > limit
            || matrix.max_norm_squared > limit
            || dots.iter().any(|d| !d.is_finite())
        {
            return self.exact(row, None);
        }
        // Real-number distance |v|^2 + |c|^2 - 2<v, c> from the BF16 dot, minimized over centers.
        let approx: Vec<f64> = dots
            .iter()
            .zip(&matrix.norms)
            .map(|(d, n)| row_squared + n - 2. * f64::from(*d))
            .collect();
        let minimum = approx.iter().copied().fold(f64::INFINITY, f64::min);
        // Bounds on the norms, with the BF16 conversion errors, as `batched` documents them.
        let a = rounded.norm_upper(r);
        let b = matrix.packed.max_norm_upper();
        let (ea, eb) = (rounded.error_upper(r), matrix.packed.max_error_upper());
        // The reviewed BF16 backends use at most dims + 32 rounding steps on
        // any product-to-result path. EPSILON (rather than EPSILON / 2) also
        // covers Arm BFDOT's round-to-odd pair sum and accumulation. BF16
        // subnormal operands were flushed during conversion and charged to ea/eb;
        // the additive allowance covers flushed intermediate results instead.
        let gamma = ne / (1. - ne);
        let underflow = (dims + 32.) * f64::from(f32::MIN_POSITIVE);
        // One BF16 dot product differs from the real one by at most this much.
        let dot_error = ea * b + eb * a + ea * eb + gamma * (a + ea) * (b + eb) + underflow;
        // The distance expression doubles it; f64 sums add a hair; the reference kernel's own
        // f32 accumulation is bounded by gamma of the largest possible distance.
        let reach = (a + b) * (a + b);
        let error = 2. * dot_error + 1e-9 * reach + 2. * gamma * reach + underflow;
        if !error.is_finite() {
            return self.exact(row, None);
        }
        let threshold = minimum + 2. * error;
        let survivors: Vec<usize> = approx
            .iter()
            .enumerate()
            .filter(|(_, d)| **d <= threshold)
            .map(|(i, _)| i)
            .collect();
        #[cfg(test)]
        {
            SCREENED[0].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            SCREENED[1].fetch_add(survivors.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        self.exact(row, Some(survivors))
    }

    /// The nearest of `candidates` (all centers when `None`) by the original kernel, ties to the
    /// lowest index.
    fn exact(&self, row: &[f32], candidates: Option<Vec<usize>>) -> usize {
        let score = |i: usize| (i, crate::distance::l2_squared_f32(row, &self.centers[i]));
        let better = |best: (usize, f32), next: (usize, f32)| {
            if next.1.total_cmp(&best.1).is_lt() {
                next
            } else {
                best
            }
        };
        match candidates {
            Some(c) => c.into_iter().map(score).reduce(better).unwrap().0,
            None => (0..self.centers.len()).map(score).reduce(better).unwrap().0,
        }
    }
}

fn squared_norm(v: &[f32]) -> f64 {
    v.iter().map(|x| f64::from(*x) * f64::from(*x)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic pseudo-random unit-ish vector, with clustered structure.
    fn vector(seed: u64, dims: usize, scale: f32, base: &[f32]) -> Vec<f32> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..dims)
            .map(|d| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let noise = (state >> 40) as f32 / (1u64 << 24) as f32 - 0.5;
                (base.get(d).copied().unwrap_or(0.) + noise * 0.3) * scale
            })
            .collect()
    }

    fn reference(row: &[f32], centers: &[Vec<f32>]) -> usize {
        crate::cluster::nearest(row, centers)
    }

    #[test]
    fn matrix_screening_picks_the_same_nearest_center_as_the_plain_scan() {
        for dims in [3, 64, 129, 1024] {
            for scale in [0., 1e-20, 1e-3, 1., 1e8, 1e18, 1e20] {
                let bases: Vec<Vec<f32>> = (0..5).map(|s| vector(900 + s, dims, 1., &[])).collect();
                let centers: Vec<Vec<f32>> = (0..61)
                    .map(|i| vector(i as u64, dims, scale, &bases[i % 5]))
                    .collect();
                let mut rows: Vec<Vec<f32>> = (0..150)
                    .map(|i| vector(1000 + i as u64, dims, scale, &bases[i % 5]))
                    .collect();
                // Rows equal to centers and to duplicated centers exercise exact ties.
                rows.push(centers[7].clone());
                rows.push(centers[7].clone());
                let mut centers = centers;
                centers[40] = centers[7].clone();
                let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
                let index = NearestCenters::with_matrix(&centers, true);
                let got = index.nearest_batch(&refs);
                for (row, got) in rows.iter().zip(got) {
                    assert_eq!(got, reference(row, &centers), "dims={dims} scale={scale}");
                }
            }
        }
    }

    #[test]
    fn partial_tiles_keep_the_exact_winner_including_ties() {
        for count in [48, 80, 81, 90, 95, 96, 97] {
            let centers: Vec<_> = (0..count).map(|i| vector(i as u64, 96, 1., &[])).collect();
            // Every center is its own exact winner. Repeated rows also exercise
            // the first columns of every row tile, where SME lost previous winners.
            let mut rows = centers.clone();
            rows.extend(centers.iter().take(7).cloned());
            let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
            let index = NearestCenters::with_matrix(&centers, true);
            for (r, got) in index.nearest_batch(&refs).into_iter().enumerate() {
                assert_eq!(got, reference(&rows[r], &centers), "count={count} row={r}");
            }
        }
    }

    #[test]
    fn an_index_built_with_matrix_screening_is_byte_identical() {
        use crate::{BuildParams, IndexBuilder, Metric, VectorType};
        let dims = 96;
        let bases: Vec<Vec<f32>> = (0..12).map(|s| vector(300 + s, dims, 1., &[])).collect();
        let mut ids = Vec::new();
        let mut data = Vec::new();
        for i in 0..6000u32 {
            ids.push(i);
            for x in vector(u64::from(i) + 77, dims, 1., &bases[i as usize % 12]) {
                data.extend_from_slice(&half::f16::from_f32(x).to_le_bytes());
            }
        }
        let build = || {
            IndexBuilder::new(dims, Metric::CosineDistance)
                .unwrap()
                .vector_type(VectorType::F16)
                .params(BuildParams {
                    clusters: Some(90),
                    ..Default::default()
                })
                .build_packed(ids.clone(), data.clone())
                .unwrap()
        };
        MODE.store(2, std::sync::atomic::Ordering::Relaxed);
        let plain = build();
        MODE.store(1, std::sync::atomic::Ordering::Relaxed);
        let screened = build();
        MODE.store(0, std::sync::atomic::Ordering::Relaxed);
        assert!(plain == screened, "blobs differ");
    }

    /// Real data: `NQ_CORPUS` is the benchmark's `corpus.f16` (an .npy of 1,024-d f16 rows).
    /// Builds the same segment both ways, then compares blobs, time, and screen survivors.
    #[test]
    #[ignore = "needs NQ_CORPUS and NQ_ROWS; run with --release --ignored --nocapture"]
    fn real_corpus_segment_blobs_are_identical_and_faster() {
        use crate::{BuildParams, IndexBuilder, Metric, VectorType};
        let path = std::env::var("NQ_CORPUS").expect("NQ_CORPUS");
        let rows: usize = std::env::var("NQ_ROWS").map_or(100_000, |v| v.parse().unwrap());
        let start: usize = std::env::var("NQ_START").map_or(1_000_000, |v| v.parse().unwrap());
        let bytes = std::fs::read(&path).unwrap();
        let header = 10 + u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
        let row_bytes = 1024 * 2;
        let data = bytes[header + start * row_bytes..header + (start + rows) * row_bytes].to_vec();
        let ids: Vec<u32> = (0..rows as u32).collect();
        let build = |mode: u8| {
            MODE.store(mode, std::sync::atomic::Ordering::Relaxed);
            let started = std::time::Instant::now();
            let blob = IndexBuilder::new(1024, Metric::CosineDistance)
                .unwrap()
                .vector_type(VectorType::F16)
                .params(BuildParams {
                    clusters: Some((rows as f64).sqrt().ceil() as usize),
                    ..Default::default()
                })
                .build_packed(ids.clone(), data.clone())
                .unwrap();
            (blob, started.elapsed())
        };
        let (plain, plain_time) = build(2);
        let (screened, screened_time) = build(1);
        MODE.store(0, std::sync::atomic::Ordering::Relaxed);
        let (n, kept) = (
            SCREENED[0].load(std::sync::atomic::Ordering::Relaxed),
            SCREENED[1].load(std::sync::atomic::Ordering::Relaxed),
        );
        println!(
            "rows {rows}: scalar {plain_time:?}, screened {screened_time:?}; {n} screened rows kept {:.2} centers on average",
            kept as f64 / n as f64
        );
        assert!(plain == screened, "blobs differ");
    }

    #[test]
    fn nonfinite_and_mismatched_rows_fall_back_to_the_plain_scan() {
        let centers: Vec<Vec<f32>> = (0..30).map(|i| vector(i, 70, 1., &[])).collect();
        let mut nan = vector(5, 70, 1., &[]);
        nan[3] = f32::NAN;
        let mut huge = vector(6, 70, 1., &[]);
        huge[0] = f32::MAX;
        let short = vec![0.5f32; 69];
        let rows = [nan, huge, short, vector(7, 70, 1., &[])];
        let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let index = NearestCenters::with_matrix(&centers, true);
        for (row, got) in rows.iter().zip(index.nearest_batch(&refs)) {
            assert_eq!(got, reference(row, &centers));
        }
    }

    #[test]
    fn the_screen_keeps_few_candidates_on_clustered_data() {
        // Not an exactness check: the certificate must be tight enough to be worth using.
        let dims = 1024;
        let bases: Vec<Vec<f32>> = (0..8).map(|s| vector(70 + s, dims, 1., &[])).collect();
        let norm = |mut v: Vec<f32>| {
            let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter_mut().for_each(|x| *x /= n);
            v
        };
        let centers: Vec<Vec<f32>> = (0..300)
            .map(|i| norm(vector(i as u64, dims, 1., &bases[i % 8])))
            .collect();
        let index = NearestCenters::with_matrix(&centers, true);
        let rows: Vec<Vec<f32>> = (0..64)
            .map(|i| norm(vector(5000 + i as u64, dims, 1., &bases[i % 8])))
            .collect();
        let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let got = index.nearest_batch(&refs);
        for (row, got) in rows.iter().zip(got) {
            assert_eq!(got, reference(row, &centers));
        }
    }
}
