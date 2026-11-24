//! Bounded matrix batches for recluster training and assignment. The build's Rayon pool owns
//! parallelism; matrixmultiply's optional thread pool is deliberately disabled.
use rayon::prelude::*;

const TRAIN_BLOCK: usize = 128;

/// Centers packed once for repeated batches of rows.
pub(super) struct Centers {
    data: Vec<f32>,
    norms: Vec<f32>,
    dims: usize,
}
impl Centers {
    pub(super) fn new<'a>(centers: impl Iterator<Item = &'a [f32]>) -> Self {
        Self::with_norms(centers.map(|center| (center, bf_vector::dots_x4([center; 4], center)[0])))
    }

    pub(super) fn with_norms<'a>(mut centers: impl Iterator<Item = (&'a [f32], f32)>) -> Self {
        let Some((first, first_norm)) = centers.next() else {
            return Self {
                data: Vec::new(),
                norms: Vec::new(),
                dims: 0,
            };
        };
        let dims = first.len();
        let count = 1 + centers.size_hint().0;
        let mut data = Vec::with_capacity(count * dims);
        let mut norms = Vec::with_capacity(count);
        for (center, norm) in std::iter::once((first, first_norm)).chain(centers) {
            assert_eq!(center.len(), dims);
            data.extend_from_slice(center);
            norms.push(norm);
        }
        Self { data, norms, dims }
    }

    pub(super) fn nearest(&self, rows: &[&[f32]]) -> Vec<usize> {
        let norms: Vec<f32> = rows
            .iter()
            .map(|v| bf_vector::dots_x4([v; 4], v)[0])
            .collect();
        self.distances(rows, &norms)
            .chunks_exact(self.norms.len())
            .map(|row| {
                row.iter()
                    .enumerate()
                    .min_by(|(ia, a), (ib, b)| a.total_cmp(b).then(ia.cmp(ib)))
                    .unwrap()
                    .0
            })
            .collect()
    }

    /// Squared distances in row-major order, with no allocation proportional to the corpus.
    pub(super) fn distances(&self, rows: &[&[f32]], norms: &[f32]) -> Vec<f32> {
        let n = self.norms.len();
        let mut out = vec![0.; rows.len() * n];
        if out.is_empty() {
            return out;
        }
        // Finite Euclidean inputs may have overflowing norms. Subtract coordinates directly in
        // that case, so identical large vectors still have distance zero instead of NaN.
        if self
            .norms
            .iter()
            .chain(norms)
            .any(|n| *n > f32::MAX / 4. || !n.is_finite())
        {
            for (row, distances) in rows.iter().zip(out.chunks_exact_mut(n)) {
                for (center, distance) in self.data.chunks_exact(self.dims).zip(distances) {
                    *distance = bf_vector::squared_distance(row, center);
                }
            }
            return out;
        }
        let mut packed = Vec::with_capacity(rows.len() * self.dims);
        for row in rows {
            assert_eq!(row.len(), self.dims);
            packed.extend_from_slice(row);
        }
        assert_eq!(rows.len(), norms.len());
        // SAFETY: A is rows x dims, B is the transpose of n x dims packed centers,
        // and C is a distinct, contiguous rows x n output. Every stride is in f32 elements.
        unsafe {
            matrixmultiply::sgemm(
                rows.len(),
                self.dims,
                n,
                -2.,
                packed.as_ptr(),
                self.dims as isize,
                1,
                self.data.as_ptr(),
                1,
                self.dims as isize,
                0.,
                out.as_mut_ptr(),
                n as isize,
                1,
            );
        }
        for (row, norm) in out.chunks_exact_mut(n).zip(norms) {
            for (d, cn) in row.iter_mut().zip(&self.norms) {
                *d += norm + cn;
            }
        }
        out
    }
}

/// The same fixed-seed Lloyd procedure as vector training, with batched nearest-center search.
/// Sums stay in input order in f64, so thread scheduling cannot change a trained index.
pub(super) fn kmeans(data: &[&[f32]], k: usize, iterations: usize, seed: u64) -> Vec<Vec<f32>> {
    let k = k.min(data.len());
    assert!(k > 0);
    let dims = data[0].len();
    let mut state = seed;
    let mut next = || {
        state = state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    };
    let mut ids: Vec<_> = (0..data.len()).collect();
    for i in 0..k {
        let j = i + next() as usize % (data.len() - i);
        ids.swap(i, j);
    }
    let mut centers: Vec<_> = ids[..k].iter().map(|i| data[*i].to_vec()).collect();
    for _ in 0..iterations {
        let packed = Centers::new(centers.iter().map(Vec::as_slice));
        let assignments: Vec<usize> = data
            .par_chunks(TRAIN_BLOCK)
            .flat_map_iter(|rows| packed.nearest(rows))
            .collect();
        let mut sums = vec![vec![0f64; dims]; k];
        let mut counts = vec![0usize; k];
        for (v, c) in data.iter().zip(assignments) {
            counts[c] += 1;
            for (sum, x) in sums[c].iter_mut().zip(*v) {
                *sum += f64::from(*x);
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                centers[c] = data[next() as usize % data.len()].to_vec();
            } else {
                for d in 0..dims {
                    centers[c][d] = (sums[c][d] / counts[c] as f64) as f32;
                }
            }
        }
    }
    centers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_distances_match_direct_distances_with_odd_shapes_and_zero_rows() {
        for dims in [1, 7, 31, 128, 1024] {
            let sample: Vec<Vec<f32>> = (0..21)
                .map(|i| {
                    (0..dims)
                        .map(|d| ((i * 7919 + d * 104729) as f64).sin() as f32)
                        .collect()
                })
                .collect();
            let centers = Centers::new(sample[..13].iter().map(Vec::as_slice));
            let rows: Vec<_> = sample[13..].iter().map(Vec::as_slice).collect();
            let norms: Vec<_> = rows
                .iter()
                .map(|v| bf_vector::dots_x4([v; 4], v)[0])
                .collect();
            let distances = centers.distances(&rows, &norms);
            for (row, got) in rows.iter().zip(distances.as_chunks::<13>().0) {
                for (center, got) in sample[..13].iter().zip(got) {
                    let expected = bf_vector::squared_distance(row, center);
                    assert!(
                        (got - expected).abs() < 1e-5 * expected.max(1.),
                        "{dims}: {got} != {expected}"
                    );
                }
            }
            assert!(centers.distances(&[], &[]).is_empty());
        }
    }

    #[test]
    fn lloyd_matches_reference_and_is_independent_of_worker_count() {
        let data: Vec<Vec<f32>> = (0..513)
            .map(|i| {
                (0..33)
                    .map(|d| ((i * 7919 + d * 104729) as f64).sin() as f32)
                    .collect()
            })
            .collect();
        let refs: Vec<_> = data.iter().map(Vec::as_slice).collect();
        let reference = bf_vector::kmeans(&refs, 11, 8, 42);
        let train = |threads| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap()
                .install(|| kmeans(&refs, 11, 8, 42))
        };
        let one = train(1);
        assert_eq!(one, reference);
        assert_eq!(one, train(2));
        let identical = [&[0., 0., 0.][..]; 8];
        assert_eq!(kmeans(&identical, 4, 8, 7), vec![vec![0.; 3]; 4]);
    }

    #[test]
    fn overflowing_norms_preserve_zero_distance_and_finite_training_centers() {
        let rows = [&[1e20, 1e20][..], &[-1e20, -1e20][..]];
        let centers = Centers::new(rows.iter().copied());
        assert_eq!(centers.nearest(&rows), [0, 1]);
        assert_eq!(kmeans(&rows, 2, 8, 42), bf_vector::kmeans(&rows, 2, 8, 42));
    }
}
