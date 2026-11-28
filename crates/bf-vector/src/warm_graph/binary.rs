//! One-bit codes: graph construction steers with them, and filtered scans screen with them. Each row keeps the signs of its rotated,
//! mean-centered coordinates. Hamming distance orders neighbors well enough to steer a beam,
//! and it reads 128 bytes per row instead of 1,024; construction scores only the nodes it
//! expands, and those the pruner needs, with the exact SQ8 kernel. The graph keeps the codes.
use super::quant::Codes;
use rayon::prelude::*;

/// Mean of the dequantized rows. Checks cancellation between row blocks.
pub(super) fn mean(
    codes: &Codes,
    cancelled: &(impl Fn() -> bool + Sync),
) -> crate::Result<Vec<f32>> {
    let dims = codes.dims;
    let n = codes.scales.len();
    let sums = codes
        .values
        .par_chunks(dims * 4096)
        .zip(codes.scales.par_chunks(4096))
        .map(|(rows, scales)| {
            let mut sum = vec![0f64; dims];
            if cancelled() {
                return sum;
            }
            for (row, &scale) in rows.chunks_exact(dims).zip(scales) {
                for (s, &x) in sum.iter_mut().zip(row) {
                    *s += f64::from(x) * f64::from(scale);
                }
            }
            sum
        })
        .reduce(
            || vec![0f64; dims],
            |mut a, b| {
                a.iter_mut().zip(b).for_each(|(a, b)| *a += b);
                a
            },
        );
    super::check(cancelled)?;
    Ok(sums.iter().map(|s| (s / n as f64) as f32).collect())
}

/// Hamming distance between two equal-length word slices (at most 4,096 bits each).
pub(super) fn hamming(a: &[u64], b: &[u64]) -> u16 {
    debug_assert_eq!(a.len(), b.len());
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is baseline on aarch64, and every load stays inside the slices.
        unsafe { hamming_neon(a, b) }
    }
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("popcnt") {
        // SAFETY: the feature was detected on this CPU.
        return unsafe { hamming_popcnt(a, b) };
    }
    #[cfg(not(target_arch = "aarch64"))]
    hamming_generic(a, b)
}
#[cfg_attr(target_arch = "aarch64", allow(dead_code))]
fn hamming_generic(a: &[u64], b: &[u64]) -> u16 {
    a.iter()
        .zip(b)
        .map(|(a, b)| (a ^ b).count_ones() as u16)
        .sum()
}
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
unsafe fn hamming_popcnt(a: &[u64], b: &[u64]) -> u16 {
    hamming_generic(a, b)
}
#[cfg(target_arch = "aarch64")]
unsafe fn hamming_neon(a: &[u64], b: &[u64]) -> u16 {
    use std::arch::aarch64::*;
    // SAFETY: each 16-byte load is inside both slices; the pairwise add widens bytes to u16 lanes, so
    // no lane overflows for rows of any supported length.
    unsafe {
        let mut sum = vdupq_n_u16(0);
        let mut i = 0;
        while i + 2 <= a.len() {
            let x = veorq_u8(
                vld1q_u8(a.as_ptr().add(i).cast()),
                vld1q_u8(b.as_ptr().add(i).cast()),
            );
            sum = vpadalq_u8(sum, vcntq_u8(x));
            i += 2;
        }
        let mut total = vaddvq_u16(sum);
        if i < a.len() {
            total += (a[i] ^ b[i]).count_ones() as u16;
        }
        total
    }
}

/// Rows the region medoids are found from, and k-modes passes over them.
const SAMPLE: usize = 65_536;
const ITERATIONS: usize = 6;

pub(super) struct Bits {
    words: Vec<u64>,
    per: usize,
}
impl Bits {
    /// Sign codes for every row of `codes`, centered on the mean of the dequantized rows.
    /// Checks cancellation between row blocks; a cancelled build returns an error.
    pub(super) fn new(
        codes: &Codes,
        cancelled: &(impl Fn() -> bool + Sync),
    ) -> crate::Result<Self> {
        let dims = codes.dims;
        let per = dims.div_ceil(64);
        let n = codes.scales.len();
        let mean = mean(codes, cancelled)?;
        let mut words = vec![0u64; n * per];
        words
            .par_chunks_mut(per)
            .zip(codes.values.par_chunks(dims))
            .zip(codes.scales.par_iter())
            .for_each(|((out, row), &scale)| {
                if cancelled() {
                    return;
                }
                for (i, (&x, &m)) in row.iter().zip(&mean).enumerate() {
                    if f32::from(x) * scale > m {
                        out[i / 64] |= 1 << (i % 64);
                    }
                }
            });
        super::check(cancelled)?;
        Ok(Self { words, per })
    }
    /// Sign bits of a rotated query, in the layout of a row. The rows were centered on their
    /// mean, and a query's dot product with the mean is the same for every row, so the query is
    /// not centered.
    pub(super) fn query(&self, rotated: &[f64]) -> Vec<u64> {
        let mut out = vec![0u64; self.per];
        for (i, x) in rotated.iter().enumerate().take(self.per * 64) {
            if *x > 0. {
                out[i / 64] |= 1 << (i % 64);
            }
        }
        out
    }
    /// Bytes retained.
    pub(super) fn bytes(&self) -> usize {
        self.words.capacity() * 8
    }
    /// Medoid nodes of about `regions` coarse regions of the rows, found by k-modes over the
    /// one-bit codes of an even sample: assignment by Hamming distance, each center the
    /// per-bit majority of its members, and each region's medoid the sample row nearest its
    /// center. Deterministic. Duplicates and empty regions are dropped, so fewer may return.
    pub(super) fn region_medoids(
        &self,
        regions: usize,
        cancelled: &(impl Fn() -> bool + Sync),
    ) -> crate::Result<Vec<u32>> {
        let n = self.words.len() / self.per.max(1);
        let sample_len = n.min(SAMPLE);
        let regions = regions.min(sample_len);
        if regions < 2 {
            return Ok(Vec::new());
        }
        let sample = (0..sample_len)
            .map(|i| (i as u64 * n as u64 / sample_len as u64) as u32)
            .collect::<Vec<_>>();
        let per = self.per;
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut centers = Vec::with_capacity(regions * per);
        for r in 0..regions {
            // Even picks, shifted a little so regions do not start on adjacent rows.
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let pick = (r * sample_len / regions + (seed % 7) as usize).min(sample_len - 1);
            centers.extend_from_slice(self.row(sample[pick]));
        }
        let nearest = |centers: &[u64], id: u32| {
            let row = self.row(id);
            centers
                .chunks_exact(per)
                .enumerate()
                .map(|(r, c)| (hamming(row, c), r))
                .min()
                .unwrap()
        };
        for _ in 0..ITERATIONS {
            super::check(cancelled)?;
            let owner = sample
                .par_iter()
                .map(|&id| nearest(&centers, id).1)
                .collect::<Vec<_>>();
            let mut votes = vec![0u32; regions * per * 64];
            let mut sizes = vec![0u32; regions];
            for (&id, &r) in sample.iter().zip(&owner) {
                sizes[r] += 1;
                let row = self.row(id);
                let votes = &mut votes[r * per * 64..(r + 1) * per * 64];
                for (w, &word) in row.iter().enumerate() {
                    let mut bits = word;
                    while bits != 0 {
                        votes[w * 64 + bits.trailing_zeros() as usize] += 1;
                        bits &= bits - 1;
                    }
                }
            }
            for r in 0..regions {
                if sizes[r] == 0 {
                    continue;
                }
                let center = &mut centers[r * per..(r + 1) * per];
                center.fill(0);
                for (b, &v) in votes[r * per * 64..(r + 1) * per * 64].iter().enumerate() {
                    if v * 2 > sizes[r] {
                        center[b / 64] |= 1 << (b % 64);
                    }
                }
            }
        }
        // Each region's medoid: the sample row nearest its center, among that region's rows.
        let mut best = vec![(u16::MAX, u32::MAX); regions];
        let owners = sample
            .par_iter()
            .map(|&id| {
                let (h, r) = nearest(&centers, id);
                (r, h, id)
            })
            .collect::<Vec<_>>();
        for (r, h, id) in owners {
            if (h, id) < best[r] {
                best[r] = (h, id);
            }
        }
        let mut ids = best
            .into_iter()
            .filter(|b| b.1 != u32::MAX)
            .map(|b| b.1)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }
    /// The packed rows of `ids`, back to back, for a pass over contiguous memory.
    pub(super) fn gather(&self, ids: &[u32]) -> Vec<u64> {
        ids.iter().flat_map(|&id| self.row(id)).copied().collect()
    }
    /// Words per row.
    pub(super) fn per(&self) -> usize {
        self.per
    }
    #[cfg(test)]
    pub(super) fn from_words(words: Vec<u64>, per: usize) -> Self {
        Self { words, per }
    }
    pub(super) fn row(&self, id: u32) -> &[u64] {
        let start = id as usize * self.per;
        &self.words[start..start + self.per]
    }
    pub(super) fn distance(&self, a: &[u64], id: u32) -> f32 {
        f32::from(hamming(a, self.row(id)))
    }
    /// Hamming distance between a query's signs and a row.
    pub(super) fn hamming(&self, query: &[u64], id: u32) -> u16 {
        hamming(query, self.row(id))
    }
    pub(super) fn prefetch(&self, id: u32) {
        let row = self.row(id);
        super::quant::prefetch_lines(row.as_ptr().cast(), row.len() * 8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hamming_kernels_match_the_scalar_count_for_every_length() {
        let mut seed = 7u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for len in 0..=64 {
            let a = (0..len).map(|_| next()).collect::<Vec<_>>();
            let b = (0..len).map(|_| next()).collect::<Vec<_>>();
            assert_eq!(hamming(&a, &b), hamming_generic(&a, &b), "len {len}");
        }
        assert_eq!(hamming(&[u64::MAX; 64], &[0; 64]), 4096);
    }
    #[test]
    fn hamming_distance_tracks_cosine_on_centered_codes() {
        let dims = 256;
        let n = 64;
        let mut seed = 99u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % 2001) as f32 / 1000. - 1.
        };
        // Row 0 is the anchor, row 1 a small perturbation of it, and the rest are independent.
        let anchor = (0..dims).map(|_| next() + 0.5).collect::<Vec<_>>();
        let mut rows = vec![anchor.clone()];
        rows.push(anchor.iter().map(|x| x + next() * 0.2).collect());
        for _ in 2..n {
            rows.push((0..dims).map(|_| next() + 0.5).collect());
        }
        let mut codes = Codes {
            dims,
            values: Vec::new(),
            scales: Vec::new(),
            dot: super::super::quant::dot_kernel(),
        };
        for row in &rows {
            let (code, scale) =
                super::super::quant::encode(&row.iter().map(|x| f64::from(*x)).collect::<Vec<_>>());
            codes.values.extend(code);
            codes.scales.push(scale);
        }
        let bits = Bits::new(&codes, &|| false).unwrap();
        let near = bits.distance(bits.row(0), 1);
        assert!(bits.distance(bits.row(0), 0) == 0.);
        for id in 2..n as u32 {
            assert!(near < bits.distance(bits.row(0), id), "row {id}");
        }
        assert!(Bits::new(&codes, &|| true).is_err());
    }
}
